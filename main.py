"""
main.py

Orquestrador desacoplado em duas fases com idempotência:
- Fase A: Descoberta RSS semanal, formação de clusters e persistência de status PENDENTE.
- Fase B: Raspagem factual resiliente em cascata (Fast HTTP + Playwright Stealth). 
"""

import os
import sys
import time
from zoneinfo import ZoneInfo

os.environ["TZ"] = "America/Sao_Paulo"
if hasattr(time, "tzset"):
    time.tzset()

FUSO_BRASILIA = ZoneInfo("America/Sao_Paulo")

import argparse
import calendar
import concurrent.futures
import gc
import glob
from datetime import datetime, timedelta
import importlib.util
import json
import random
import re
import subprocess
import types

from notifier import enviar_telegram
import processor


def gerar_meses_entre(inicio_mmaaaa: str, fim_mmaaaa: str) -> list:
    """Gera lista de tuplas (ano, mes) entre duas marcas MMAAAA inclusivas."""
    ini = inicio_mmaaaa.strip().replace("/", "").replace("-", "")
    fim = fim_mmaaaa.strip().replace("/", "").replace("-", "")
    m_ini, a_ini = int(ini[:2]), int(ini[2:])
    m_fim, a_fim = int(fim[:2]), int(fim[2:])

    meses = []
    dt_atual = datetime(a_ini, m_ini, 1)
    dt_fim = datetime(a_fim, m_fim, 1)

    while dt_atual <= dt_fim:
        meses.append((dt_atual.year, dt_atual.month))
        ultimo_dia = calendar.monthrange(dt_atual.year, dt_atual.month)[1]
        dt_atual = dt_atual + timedelta(days=ultimo_dia)
        dt_atual = dt_atual.replace(day=1)

    return meses


def gerar_subjanelas_mes(ano: int, mes: int) -> list:
    """Divide o mês em 4 intervalos de datas (dt_ini, dt_fim) para varredura RSS."""
    ultimo_dia = calendar.monthrange(ano, mes)[1]
    cortes = [(1, 7), (8, 14), (15, 21), (22, ultimo_dia)]
    janelas = []
    for d_ini, d_fim in cortes:
        dt_ini = datetime(ano, mes, d_ini)
        if d_fim == ultimo_dia:
            dt_fim = datetime(ano + 1, 1, 1) if mes == 12 else datetime(ano, mes + 1, 1)
        else:
            dt_fim = datetime(ano, mes, d_fim + 1)
        janelas.append((dt_ini, dt_fim))
    return janelas
    

def _coletar_janela_adaptativa(
    query_base: str,
    dt_ini: datetime,
    dt_fim: datetime,
    hl: str,
    gl: str,
    timeout_req: int,
    teto_truncamento: int = 95,
) -> list:
    """
    Consulta o RSS do Google News para o intervalo [dt_ini, dt_fim).
    Caso o retorno atinja o teto de truncamento do Google (~100 itens) e o
    intervalo seja maior que 1 dia, bissecta o período recursivamente.
    """
    filtro = f"after:{dt_ini.strftime('%Y-%m-%d')} before:{dt_fim.strftime('%Y-%m-%d')}"
    query_final = f"{query_base} {filtro}".strip()
    itens = processor.fetch_rss_feed(
        query_final, hl=hl, gl=gl, timeout=timeout_req, dt_ini=dt_ini, dt_fim=dt_fim
    )

    dias_intervalo = (dt_fim - dt_ini).days
    if len(itens) >= teto_truncamento and dias_intervalo > 1:
        dt_meio = dt_ini + timedelta(days=dias_intervalo // 2)
        itens_esq = _coletar_janela_adaptativa(
            query_base, dt_ini, dt_meio, hl, gl, timeout_req, teto_truncamento
        )
        itens_dir = _coletar_janela_adaptativa(
            query_base, dt_meio, dt_fim, hl, gl, timeout_req, teto_truncamento
        )
        return itens_esq + itens_dir

    return itens


def carregar_modulo_config(caminho_ou_shard: str):
    """Carrega dinamicamente a configuração do Google Drive via Rclone streaming."""
    if os.path.exists(caminho_ou_shard):
        spec = importlib.util.spec_from_file_location("config_modulo", caminho_ou_shard)
        modulo = importlib.util.module_from_spec(spec)
        sys.modules["config_modulo"] = modulo
        spec.loader.exec_module(modulo)
        return modulo

    nome_arquivo = f"config_{caminho_ou_shard}.py" if not caminho_ou_shard.endswith(".py") else caminho_ou_shard
    comando = ["rclone", "cat", f"gdrive_config:{nome_arquivo}"]
    proc = subprocess.run(comando, capture_output=True)
    if proc.returncode != 0:
        erro_msg = proc.stderr.decode("utf-8", errors="replace")
        raise RuntimeError(f"Falha ao ler {nome_arquivo} do Google Drive: {erro_msg}")

    conteudo = None
    for enc in ["utf-8-sig", "utf-8", "utf-16", "latin-1"]:
        try:
            conteudo = proc.stdout.decode(enc)
            break
        except UnicodeDecodeError:
            continue

    if conteudo is None:
        raise ValueError(f"Não foi possível decodificar {nome_arquivo}.")

    modulo = types.ModuleType("config_modulo")
    modulo.__file__ = f"<gdrive_config:{nome_arquivo}>"
    sys.modules["config_modulo"] = modulo
    exec(conteudo, modulo.__dict__)
    return modulo


def carregar_mapa_inventario_drive() -> dict:
    """
    Obtém em uma única chamada de rede os nomes e tamanhos de todos os arquivos
    na pasta 'dados_brutos' do Google Drive via Rclone.
    """
    comando = ["rclone", "lsjson", "gdrive_dados:", "--files-only"]
    proc = subprocess.run(comando, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Falha ao inventariar 'dados_brutos' via Rclone: {proc.stderr}")
    try:
        itens = json.loads(proc.stdout)
        return {item["Name"]: item["Size"] for item in itens}
    except Exception as exc:
        raise RuntimeError(f"Erro ao analisar JSON de retorno do Rclone: {exc}")


def _extrair_cluster_worker(cluster: dict) -> dict:
    """Executa a extração HTTP de um cluster em processo isolado para proteção de heap C."""
    time.sleep(random.uniform(0.05, 0.2))
    return processor.process_cluster_with_fallback(cluster, timeout=6)


def executar_fase_a_descoberta(
    cfg,
    shard_id: str,
    meses_alvo: list,
    mapa_drive: dict,
    tema_filtro: str = None,
    forcar_redescoberta: bool = False,
) -> int:
    """Coleta metadados RSS do shard e persiste localmente para consolidação posterior."""
    print(f"\n{'='*65}\nINICIANDO FASE A (DESCOBERTA RSS): [{shard_id.upper()}]\n{'='*65}", flush=True)
    mercados = cfg.MERCADOS_ALVO
    timeout_req = getattr(cfg, "TIMEOUT_REQUISICAO", 6)
    dominios = getattr(cfg, "DOMINIOS_PREFERENCIAIS", [])
    slug_filtro = processor.normalizar_nome_tema(tema_filtro) if tema_filtro else None

    artigos_por_arquivo = {}
    mapa_slug_tema = {}
    total_descobertos = 0

    for ano, mes in meses_alvo:
        tag_mes = f"{ano:04d}_{mes:02d}"
        subjanelas = gerar_subjanelas_mes(ano, mes)

        for tema, dict_idiomas in cfg.MONITORAMENTOS.items():
            tema_slug = processor.normalizar_nome_tema(tema)
            if slug_filtro and tema_slug != slug_filtro:
                continue

            mapa_slug_tema[tema_slug] = tema
            nome_arquivo = f"noticias_raw_{tag_mes}_{tema_slug}.json"

            if nome_arquivo not in mapa_drive:
                continue

            tamanho = mapa_drive.get(nome_arquivo, 0)
            if tamanho > 4 and not forcar_redescoberta:
                proc_check = subprocess.run(["rclone", "cat", f"gdrive_dados:{nome_arquivo}"], capture_output=True)
                if proc_check.returncode == 0:
                    dados_existentes = json.loads(proc_check.stdout.decode("utf-8"))
                    if isinstance(dados_existentes, list) and len(dados_existentes) > 0:
                        if dados_existentes[0].get("status_extracao") != "SEM_NOTICIAS_NO_PERIODO":
                            continue

            raw_artigos = []
            for dt_ini, dt_fim in subjanelas:
                for mercado in mercados:
                    gl, hl, lang = mercado["gl"], mercado["hl"], mercado["lang"]
                    termos = dict_idiomas.get(lang, [])
                    excluidos = getattr(cfg, "TERMOS_EXCLUIDOS", {}).get(lang, [])

                    for termo in termos:
                        query_base = processor.build_rss_query(termo, excluidos, dominios)
                        itens = _coletar_janela_adaptativa(
                            query_base=query_base,
                            dt_ini=dt_ini,
                            dt_fim=dt_fim,
                            hl=hl,
                            gl=gl,
                            timeout_req=timeout_req,
                        )
                        for item in itens:
                            item["tema"] = tema
                            item["termo_origem"] = termo
                            item["pais_emissao"] = gl
                            item["idioma"] = hl
                            raw_artigos.append(item)

            if raw_artigos:
                artigos_por_arquivo[nome_arquivo] = raw_artigos
                total_descobertos += len(raw_artigos)
                print(f"   + [{shard_id}] {nome_arquivo}: {len(raw_artigos)} itens brutos coletados.", flush=True)

    arquivo_saida_shard = f"shard_raw_{shard_id}.json"
    payload_shard = {
        "shard": shard_id,
        "mapa_slug_tema": mapa_slug_tema,
        "artigos_por_arquivo": artigos_por_arquivo,
    }
    with open(arquivo_saida_shard, "w", encoding="utf-8") as f:
        json.dump(payload_shard, f, ensure_ascii=False, indent=2)

    print(f">> Fase A concluída para [{shard_id.upper()}]. Total bruto capturado: {total_descobertos}\n", flush=True)
    return total_descobertos


def executar_fase_a_consolidar(
    meses_alvo: list,
    tema_filtro: str = None,
    forcar_redescoberta: bool = False,
) -> int:
    """Unifica os resultados de todos os shards da Fase A e grava no Google Drive sem concorrência."""
    print(f"\n{'='*65}\nCONSOLIDANDO SHARDS DA FASE A E PERSISTINDO NO DRIVE\n{'='*65}", flush=True)
    mapa_drive = carregar_mapa_inventario_drive()
    slug_filtro = processor.normalizar_nome_tema(tema_filtro) if tema_filtro else None

    arquivos_shards = sorted(glob.glob("shard_raw_*.json"))
    if not arquivos_shards:
        raise FileNotFoundError("Nenhum artifact 'shard_raw_*.json' localizado para consolidação.")

    agregado_artigos = {}
    mapa_slug_tema = {}
    for arq_s in arquivos_shards:
        with open(arq_s, "r", encoding="utf-8") as f:
            dados_s = json.load(f)
        mapa_slug_tema.update(dados_s.get("mapa_slug_tema", {}))
        for nome_arq, lista_itens in dados_s.get("artigos_por_arquivo", {}).items():
            agregado_artigos.setdefault(nome_arq, []).extend(lista_itens)

    tags_meses = {f"{ano:04d}_{mes:02d}" for ano, mes in meses_alvo}
    arquivos_alvo = []
    for nome_arq in sorted(mapa_drive.keys()):
        m = re.match(r"^noticias_raw_(\d{4}_\d{2})_(.+)\.json$", nome_arq)
        if not m:
            continue
        tag_m, slug_t = m.group(1), m.group(2)
        if tag_m not in tags_meses:
            continue
        if slug_filtro and slug_t != slug_filtro:
            continue
        arquivos_alvo.append((nome_arq, tag_m, slug_t))

    total_gravados = 0
    for nome_arquivo, tag_mes, tema_slug in arquivos_alvo:
        tamanho = mapa_drive.get(nome_arquivo, 0)
        clusters_existentes = []

        if tamanho > 4 and not forcar_redescoberta:
            proc_check = subprocess.run(["rclone", "cat", f"gdrive_dados:{nome_arquivo}"], capture_output=True)
            if proc_check.returncode == 0:
                dados_ex = json.loads(proc_check.stdout.decode("utf-8"))
                if isinstance(dados_ex, list) and len(dados_ex) > 0:
                    if dados_ex[0].get("status_extracao") != "SEM_NOTICIAS_NO_PERIODO":
                        print(f"⏭️ [MANTIDO] {nome_arquivo} já possui {len(dados_ex)} clusters válidos.", flush=True)
                        continue

        raw_combinado = agregado_artigos.get(nome_arquivo, [])
        tema_nome = mapa_slug_tema.get(tema_slug, tema_slug)

        if not raw_combinado:
            conteudo_final = [{
                "status_extracao": "SEM_NOTICIAS_NO_PERIODO",
                "tema": tema_nome,
                "ano_mes": tag_mes,
                "timestamp": datetime.now(FUSO_BRASILIA).strftime("%Y-%m-%d %H:%M:%S"),
            }]
        else:
            prefixo = f"CLUS_{tag_mes}_{tema_slug[:4].upper()}"
            novos_clusters = processor.cluster_articles_rapido(
                raw_combinado, similarity_threshold=92.0, prefixo_id=prefixo
            )
            urls_mapeadas = {c["url_primaria"] for c in clusters_existentes if "url_primaria" in c}
            conteudo_final = list(clusters_existentes)

            for c in novos_clusters:
                if c["url_primaria"] not in urls_mapeadas:
                    c["id_cluster"] = f"{prefixo}_{len(conteudo_final) + 1:04d}"
                    conteudo_final.append({
                        "id_cluster": c["id_cluster"],
                        "tema": c["tema"],
                        "termo_origem": c["termo_origem"],
                        "titulo": c["titulo"],
                        "data_noticia": c["data_noticia"],
                        "idioma": c["idioma"],
                        "fonte_utilizada": c["fonte_principal"],
                        "url_utilizada": c["url_primaria"],
                        "url_primaria": c["url_primaria"],
                        "urls_espelho": c["urls_espelho"],
                        "urls_espelho_disponiveis": c["urls_espelho"],
                        "urls_espelho_canonicas": [],
                        "url_canonica_resolvida": None,
                        "status_resolucao": "PENDENTE",
                        "status_extracao": "PENDENTE",
                        "texto_completo": "",
                        "motivo_bloqueio": None,
                        "necessita_extracao_manual": False,
                        "historico_tentativas": [],
                        "vetor_1024": None,
                    })
                    urls_mapeadas.add(c["url_primaria"])

        with open(nome_arquivo, "w", encoding="utf-8") as f:
            json.dump(conteudo_final, f, ensure_ascii=False, indent=2)

        res = subprocess.run(["rclone", "copyto", nome_arquivo, f"gdrive_dados:{nome_arquivo}"], capture_output=True, text=True)
        if res.returncode != 0:
            raise RuntimeError(f"Falha ao gravar {nome_arquivo} no Drive: {res.stderr}")

        total_gravados += len(conteudo_final)
        print(f"   ✓ [CONSOLIDADO] {nome_arquivo}: {len(conteudo_final)} registros gravados no Drive.", flush=True)

    return total_gravados


def executar_fase_b_extracao(
    meses_alvo: list,
    tema_filtro: str = None,
    worker_index: int = 0,
    total_workers: int = 1,
) -> int:
    """Executa a Fase B particionada por índice de arquivo, eliminando sobreposição entre workers."""
    print(
        f"\n{'='*65}\nINICIANDO FASE B (WORKER {worker_index + 1}/{total_workers})\n{'='*65}",
        flush=True,
    )
    mapa_drive = carregar_mapa_inventario_drive()
    slug_filtro = processor.normalizar_nome_tema(tema_filtro) if tema_filtro else None
    tags_meses = {f"{ano:04d}_{mes:02d}" for ano, mes in meses_alvo}

    todos_arquivos = []
    for nome_arq in sorted(mapa_drive.keys()):
        m = re.match(r"^noticias_raw_(\d{4}_\d{2})_(.+)\.json$", nome_arq)
        if not m:
            continue
        tag_m, slug_t = m.group(1), m.group(2)
        if tag_m not in tags_meses:
            continue
        if slug_filtro and slug_t != slug_filtro:
            continue
        todos_arquivos.append(nome_arq)

    arquivos_worker = todos_arquivos[worker_index::total_workers]
    print(f"Arquivos atribuídos a este worker: {len(arquivos_worker)} de {len(todos_arquivos)}.", flush=True)

    total_clusters_lote = 0
    total_processadas_lote = 0
    total_sucessos_lote = 0

    for nome_arquivo in arquivos_worker:
        tamanho = mapa_drive.get(nome_arquivo, 0)
        if tamanho <= 4:
            continue

        proc_cat = subprocess.run(["rclone", "cat", f"gdrive_dados:{nome_arquivo}"], capture_output=True)
        if proc_cat.returncode != 0:
            raise RuntimeError(f"Erro ao ler {nome_arquivo} do Drive: {proc_cat.stderr.decode('utf-8', errors='replace')}")

        clusters = json.loads(proc_cat.stdout.decode("utf-8"))
        if not isinstance(clusters, list) or len(clusters) == 0:
            continue

        if clusters[0].get("status_extracao") == "SEM_NOTICIAS_NO_PERIODO":
            continue

        itens_pendentes = [c for c in clusters if c.get("status_extracao") in ["PENDENTE", "CONTEUDO_BLOQUEADO"]]
        if not itens_pendentes:
            continue

        print(f"\n>> [FASE B] Extraindo {len(itens_pendentes)} clusters em: {nome_arquivo}...", flush=True)
        gc.collect()

        mapa_resolvidos = {}
        with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
            futuros = {executor.submit(_extrair_cluster_worker, c): c for c in itens_pendentes}
            for f in concurrent.futures.as_completed(futuros):
                res_c = f.result()
                mapa_resolvidos[res_c["id_cluster"]] = res_c

        bloqueados = [item for item in mapa_resolvidos.values() if item.get("status_extracao") != "SUCESSO"]
        if bloqueados:
            print(f"   Ativando Playwright Stealth para {len(bloqueados)} matérias retidas...", flush=True)
            recuperados = processor.executar_fallback_playwright(bloqueados)
            for rec in recuperados:
                mapa_resolvidos[rec["id_cluster"]] = rec

        for idx, c in enumerate(clusters):
            cid = c.get("id_cluster")
            if cid in mapa_resolvidos:
                clusters[idx] = mapa_resolvidos[cid]

        sucessos_arquivo = sum(1 for c in clusters if c.get("status_extracao") == "SUCESSO")
        total_clusters_lote += len(clusters)
        total_processadas_lote += len(itens_pendentes)
        total_sucessos_lote += sucessos_arquivo
        print(f"   Resultado: {sucessos_arquivo}/{len(clusters)} textos válidos em {nome_arquivo}.", flush=True)

        with open(nome_arquivo, "w", encoding="utf-8") as f:
            json.dump(clusters, f, ensure_ascii=False, indent=2)

        res_copy = subprocess.run(["rclone", "copyto", nome_arquivo, f"gdrive_dados:{nome_arquivo}"], capture_output=True, text=True)
        if res_copy.returncode != 0:
            raise RuntimeError(f"Falha ao salvar Fase B em {nome_arquivo}: {res_copy.stderr}")

    if total_processadas_lote > 0 and os.getenv("TELEGRAM_BOT_TOKEN"):
        enviar_telegram(
            regiao_nome=f"BACKFILL WORKER {worker_index + 1}/{total_workers}",
            arquivos=[],
            total_brutas=total_clusters_lote,
            total_clusters=total_clusters_lote,
            total_processadas=total_processadas_lote,
            sucessos=total_sucessos_lote,
            bloqueados=total_clusters_lote - total_sucessos_lote,
        )

    print(f">> Worker {worker_index + 1}/{total_workers} concluído. Sucessos acumulados: {total_sucessos_lote}\n", flush=True)
    return total_sucessos_lote


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestrador Multirregional Resiliente (Backfill)")
    parser.add_argument("--config", type=str, default=None)
    parser.add_argument("--shard", type=str, default=None)
    parser.add_argument("--inicio_mmaaaa", type=str, default=None, help="MMAAAA inicial (ex: 102021)")
    parser.add_argument("--fim_mmaaaa", type=str, default=None, help="MMAAAA final (ex: 112026)")
    parser.add_argument("--tema", type=str, default=None, help="Filtro opcional de tema único (ex: itau)")
    parser.add_argument("--fase", type=str, default="todas", choices=["a", "consolidar-a", "b", "todas"])
    parser.add_argument("--forcar-redescoberta", action="store_true")
    parser.add_argument("--worker-index", type=int, default=0)
    parser.add_argument("--total-workers", type=int, default=1)
    args = parser.parse_args()

    if args.inicio_mmaaaa and args.fim_mmaaaa:
        meses_escopo = gerar_meses_entre(args.inicio_mmaaaa, args.fim_mmaaaa)
    else:
        agora = datetime.now(FUSO_BRASILIA)
        meses_escopo = [(agora.year, agora.month)]

    if args.fase == "a":
        alvo = args.shard if args.shard else args.config
        if not alvo:
            raise ValueError("É obrigatório informar --shard ou --config para a Fase A.")
        cfg = carregar_modulo_config(alvo)
        shard_id = os.path.basename(alvo).replace("config_", "").replace(".py", "").strip().lower()
        mapa = carregar_mapa_inventario_drive()
        executar_fase_a_descoberta(
            cfg=cfg,
            shard_id=shard_id,
            meses_alvo=meses_escopo,
            mapa_drive=mapa,
            tema_filtro=args.tema,
            forcar_redescoberta=args.forcar_redescoberta,
        )
    elif args.fase == "consolidar-a":
        executar_fase_a_consolidar(
            meses_alvo=meses_escopo,
            tema_filtro=args.tema,
            forcar_redescoberta=args.forcar_redescoberta,
        )
    elif args.fase == "b":
        executar_fase_b_extracao(
            meses_alvo=meses_escopo,
            tema_filtro=args.tema,
            worker_index=args.worker_index,
            total_workers=args.total_workers,
        )
