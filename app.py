import streamlit as st
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import time
from datetime import datetime, time as dt_time
from validate_docbr import CPF
import calendar
from dateutil.relativedelta import relativedelta
import altair as alt

# -----------------------------------------------------
# CONFIGURAÇÃO E CONEXÃO
# -----------------------------------------------------
st.set_page_config(layout="wide", page_title="Studio Pilates App")


@st.cache_resource(ttl=300)
def connect_to_sheets():
    """Conecta ao Google Sheets usando as credenciais do Streamlit."""
    try:
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds_dict = st.secrets["gcp_service_account"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sheet = client.open("StudioPilatesDB")
        return sheet
    except Exception as e:
        st.error(f"Erro ao conectar com o Google Sheets: {e}")
        st.error("Verifique o 'secrets.toml' e as permissões de compartilhamento.")
        return None


sheet = connect_to_sheets()

hoje = datetime.now()
MES_ATUAL = hoje.month
ANO_ATUAL = hoje.year
LISTA_MESES_NOMES = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}


# -----------------------------------------------------
# FUNÇÕES DE CARREGAMENTO DE DADOS (CACHE)
# -----------------------------------------------------

@st.cache_data(ttl=300)
def load_data(worksheet_name):
    """Função genérica para carregar uma aba como DataFrame (lendo como texto)."""
    try:
        ws = sheet.worksheet(worksheet_name)
        all_values = ws.get_all_values()
        if not all_values:
            return pd.DataFrame()

        headers = all_values[0]
        records_data = all_values[1:]
        df = pd.DataFrame(records_data, columns=headers)

        if not df.empty:
            df.columns = df.columns.str.strip()
        return df
    except gspread.exceptions.WorksheetNotFound:
        st.error(f"Aba '{worksheet_name}' não encontrada!")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro ao carregar dados da aba '{worksheet_name}': {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_matriculas():
    """Carrega e limpa dados da aba Matrículas."""
    df = load_data("Matriculas")
    if not df.empty:
        if 'ID' in df.columns:
            df['ID'] = pd.to_numeric(df['ID'], errors='coerce')
        if 'Data_Inicio' in df.columns:
            df['Data_Inicio'] = pd.to_datetime(df['Data_Inicio'], errors='coerce').dt.date
            df['Data_Inicio'] = pd.to_datetime(df['Data_Inicio'], errors='coerce')

        if 'Data_Nascimento' in df.columns:
            df['Data_Nascimento'] = pd.to_datetime(df['Data_Nascimento'], errors='coerce').dt.date
            df['Data_Nascimento'] = pd.to_datetime(df['Data_Nascimento'], errors='coerce')
        else:
            df['Data_Nascimento'] = pd.NaT

        if 'Desconto_Percentual' in df.columns:
            df['Desconto_Percentual'] = pd.to_numeric(df['Desconto_Percentual'], errors='coerce').fillna(0.0)
        else:
            df['Desconto_Percentual'] = 0.0

        if 'Justificativa_Desconto' not in df.columns:
            df['Justificativa_Desconto'] = ''

        if 'Data_Congelamento_Inicio' in df.columns:
            df['Data_Congelamento_Inicio'] = pd.to_datetime(df['Data_Congelamento_Inicio'], errors='coerce').dt.date
        else:
            df['Data_Congelamento_Inicio'] = pd.NaT

        if 'Data_Primeira_Matricula' in df.columns:
            df['Data_Primeira_Matricula'] = pd.to_datetime(df['Data_Primeira_Matricula'], errors='coerce').dt.date
            df['Data_Primeira_Matricula'] = pd.to_datetime(df['Data_Primeira_Matricula'], errors='coerce')
        else:
            df['Data_Primeira_Matricula'] = pd.NaT

    return df


@st.cache_data(ttl=300)
def load_planos():
    """Carrega e limpa dados da aba Planos."""
    df = load_data("Planos")
    if not df.empty:
        if 'Preco_Mensal' in df.columns:
            df['Preco_Mensal'] = df['Preco_Mensal'].astype(str).str.replace('R$', '', regex=False).str.strip()
            df['Preco_Mensal'] = df['Preco_Mensal'].str.replace(',', '.', regex=False)
            df['Preco_Mensal'] = df['Preco_Mensal'].apply(
                lambda x: x.replace('.', '', x.count('.') - 1) if x.count('.') > 1 else x)
            df['Preco_Mensal'] = pd.to_numeric(df['Preco_Mensal'], errors='coerce').fillna(0.0)
        if 'Duracao_Meses' in df.columns:
            df['Duracao_Meses'] = pd.to_numeric(df['Duracao_Meses'], errors='coerce').fillna(0)
    return df


@st.cache_data(ttl=300)
def load_despesas():
    """Carrega e limpa dados da aba Lancamentos_Despesas (Contas a Pagar)."""
    df = load_data("Lancamentos_Despesas")
    if not df.empty:
        if 'ID' in df.columns:
            df['ID'] = pd.to_numeric(df['ID'].astype(str).str.strip(), errors='coerce').fillna(0).astype(int)

        if 'Valor' in df.columns:
            df['Valor'] = df['Valor'].astype(str).str.replace('R$', '', regex=False).str.strip()
            df['Valor'] = df['Valor'].str.replace(',', '.', regex=False)
            df['Valor'] = df['Valor'].apply(lambda x: x.replace('.', '', x.count('.') - 1) if x.count('.') > 1 else x)
            df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce').fillna(0.0)

        if 'Mes_Competencia' in df.columns:
            df['Mes_Competencia'] = pd.to_numeric(df['Mes_Competencia'].astype(str).str.strip(),
                                                  errors='coerce').fillna(0).astype(int)
        if 'Ano_Competencia' in df.columns:
            df['Ano_Competencia'] = pd.to_numeric(df['Ano_Competencia'].astype(str).str.strip(),
                                                  errors='coerce').fillna(0).astype(int)

        if 'Valor_Pago' in df.columns:
            df['Valor_Pago'] = df['Valor_Pago'].astype(str).str.replace('R$', '', regex=False).str.strip()
            df['Valor_Pago'] = df['Valor_Pago'].str.replace(',', '.', regex=False)
            df['Valor_Pago'] = df['Valor_Pago'].apply(
                lambda x: x.replace('.', '', x.count('.') - 1) if x.count('.') > 1 else x)
            df['Valor_Pago'] = pd.to_numeric(df['Valor_Pago'], errors='coerce').fillna(0.0)
        else:
            df['Valor_Pago'] = 0.0

        if 'Data_Pagamento' in df.columns:
            df['Data_Pagamento'] = pd.to_datetime(df['Data_Pagamento'], errors='coerce')
        else:
            df['Data_Pagamento'] = pd.NaT

        if 'Data_Vencimento' in df.columns:
            df['Data_Vencimento'] = pd.to_datetime(df['Data_Vencimento'], errors='coerce')
        else:
            df['Data_Vencimento'] = pd.NaT

        if 'Status_Pagamento' not in df.columns:
            df['Status_Pagamento'] = 'Pendente'
        if 'Forma_Pagamento' not in df.columns:
            df['Forma_Pagamento'] = ''
        if 'Recorrente' not in df.columns:
            df['Recorrente'] = 'Não'

        if 'Ano_Competencia' in df.columns and 'Mes_Competencia' in df.columns:
            df['Data_Competencia'] = pd.to_datetime(
                df['Ano_Competencia'].astype(str) + '-' +
                df['Mes_Competencia'].astype(str) + '-01',
                format='%Y-%m-%d',
                errors='coerce'
            )
    return df


@st.cache_data(ttl=300)
def load_presencas():
    """Carrega e limpa dados da aba Presencas_Evolucao."""
    df = load_data("Presencas_Evolucao")
    if not df.empty:
        if 'ID_Presenca' in df.columns:
            df['ID_Presenca'] = pd.to_numeric(df['ID_Presenca'], errors='coerce')
        if 'ID_Aluno' in df.columns:
            df['ID_Aluno'] = pd.to_numeric(df['ID_Aluno'], errors='coerce')
        if 'Data_Aula' in df.columns:
            df['Data_Aula'] = pd.to_datetime(df['Data_Aula'], errors='coerce')
    return df


@st.cache_data(ttl=300)
def load_pagamentos():
    """Carrega e limpa dados da aba Pagamentos_Recebidos."""
    df = load_data("Pagamentos_Recebidos")
    if not df.empty:
        if 'ID_Pagamento' in df.columns:
            df['ID_Pagamento'] = pd.to_numeric(df['ID_Pagamento'], errors='coerce')
        if 'ID_Aluno' in df.columns:
            df['ID_Aluno'] = pd.to_numeric(df['ID_Aluno'], errors='coerce')
        if 'Data_Pagamento' in df.columns:
            df['Data_Pagamento'] = pd.to_datetime(df['Data_Pagamento'], errors='coerce')

        if 'Mes_Competencia' in df.columns:
            df['Mes_Competencia'] = pd.to_numeric(df['Mes_Competencia'].astype(str).str.strip(),
                                                  errors='coerce').fillna(0).astype(int)
        if 'Ano_Competencia' in df.columns:
            df['Ano_Competencia'] = pd.to_numeric(df['Ano_Competencia'].astype(str).str.strip(),
                                                  errors='coerce').fillna(0).astype(int)

        # Limpeza do Valor_Pago (Bruto)
        if 'Valor_Pago' in df.columns:
            df['Valor_Pago'] = df['Valor_Pago'].astype(str).str.replace('R$', '', regex=False).str.strip()
            df['Valor_Pago'] = df['Valor_Pago'].str.replace(',', '.', regex=False)
            df['Valor_Pago'] = df['Valor_Pago'].apply(
                lambda x: x.replace('.', '', x.count('.') - 1) if x.count('.') > 1 else x)
            df['Valor_Pago'] = pd.to_numeric(df['Valor_Pago'], errors='coerce').fillna(0.0)

        # Limpeza do Valor_Liquido
        if 'Valor_Liquido' in df.columns:
            df['Valor_Liquido'] = df['Valor_Liquido'].astype(str).str.replace('R$', '', regex=False).str.strip()
            df['Valor_Liquido'] = df['Valor_Liquido'].str.replace(',', '.', regex=False)
            df['Valor_Liquido'] = df['Valor_Liquido'].apply(
                lambda x: x.replace('.', '', x.count('.') - 1) if x.count('.') > 1 else x)
            df['Valor_Liquido'] = pd.to_numeric(df['Valor_Liquido'], errors='coerce')
        else:
            df['Valor_Liquido'] = pd.NA

        # Lógica Chave: Se Valor_Liquido for Nulo ou 0, ele é igual ao Valor_Pago (Bruto).
        df['Valor_Liquido'] = df['Valor_Liquido'].fillna(df['Valor_Pago'])
        df['Valor_Liquido'] = df.apply(
            lambda row: row['Valor_Pago'] if row['Valor_Liquido'] == 0 else row['Valor_Liquido'],
            axis=1
        )

    return df


@st.cache_data(ttl=300)
def load_investimentos():
    """Carrega e limpa dados da aba Investimentos_Caixa."""
    df = load_data("Investimentos_Caixa")
    if not df.empty:
        if 'ID_Movimentacao' in df.columns:
            df['ID_Movimentacao'] = pd.to_numeric(df['ID_Movimentacao'].astype(str).str.strip(),
                                                  errors='coerce').fillna(0).astype(int)
        if 'Data' in df.columns:
            df['Data'] = pd.to_datetime(df['Data'], errors='coerce')

        if 'Valor' in df.columns:
            df['Valor'] = df['Valor'].astype(str).str.replace('R$', '', regex=False).str.strip()
            df['Valor'] = df['Valor'].str.replace(',', '.', regex=False)
            df['Valor'] = df['Valor'].apply(lambda x: x.replace('.', '', x.count('.') - 1) if x.count('.') > 1 else x)
            df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce').fillna(0.0)
    return df


@st.cache_data(ttl=300)
def load_historico_renovacoes():
    """Carrega dados da aba Historico_Renovacoes."""
    df = load_data("Historico_Renovacoes")
    if not df.empty:
        df['ID_Historico'] = pd.to_numeric(df['ID_Historico'], errors='coerce')
        df['ID_Aluno'] = pd.to_numeric(df['ID_Aluno'], errors='coerce')
        df['Data_Inicio_Contrato'] = pd.to_datetime(df['Data_Inicio_Contrato'], errors='coerce').dt.date
        df['Data_Inicio_Contrato'] = pd.to_datetime(df['Data_Inicio_Contrato'], errors='coerce')

        if 'Valor_Contrato' in df.columns:
            df['Valor_Contrato'] = df['Valor_Contrato'].astype(str).str.replace('R$', '', regex=False).str.strip()
            df['Valor_Contrato'] = df['Valor_Contrato'].str.replace(',', '.', regex=False)
            df['Valor_Contrato'] = df['Valor_Contrato'].apply(
                lambda x: x.replace('.', '', x.count('.') - 1) if x.count('.') > 1 else x)
            df['Valor_Contrato'] = pd.to_numeric(df['Valor_Contrato'], errors='coerce').fillna(0.0)
    return df


@st.cache_data(ttl=300)
def load_taxas():
    """Carrega e limpa dados da aba Config_Taxas."""
    df = load_data("Config_Taxas")
    if not df.empty:
        if 'Taxa' in df.columns:
            # Limpa R$, , e converte para número
            df['Taxa'] = df['Taxa'].astype(str).str.replace('R$', '', regex=False).str.strip()
            df['Taxa'] = df['Taxa'].str.replace(',', '.', regex=False)
            df['Taxa'] = pd.to_numeric(df['Taxa'], errors='coerce').fillna(0.0)
            df['Taxa'] = df['Taxa'] / 100.0
        else:
            st.error("Coluna 'Taxa' não encontrada na aba 'Config_Taxas'!")
            df['Taxa'] = 0.0

        # Garante limpeza completa das strings (remove espaços ocultos)
        for col in ['Bandeira', 'Tipo', 'Parcela']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
            else:
                st.error(f"Coluna '{col}' não encontrada na aba 'Config_Taxas'!")

        # Remove linhas vazias se houver
        df = df[df['Bandeira'] != ""]

    return df


def clear_all_caches():
    """Limpa todos os caches de dados do app."""
    load_data.clear()
    load_matriculas.clear()
    load_planos.clear()
    load_despesas.clear()
    load_presencas.clear()
    load_pagamentos.clear()
    load_investimentos.clear()
    load_historico_renovacoes.clear()
    load_taxas.clear()


# -----------------------------------------------------
# FUNÇÕES HELPER DE ATUALIZAÇÃO (Google Sheets)
# -----------------------------------------------------
def atualizar_matricula_aluno(id_aluno, dados_para_atualizar):
    """Atualiza uma linha específica na aba 'Matriculas' com base no ID."""
    try:
        matriculas_ws = sheet.worksheet("Matriculas")
        df_matriculas_raw = pd.DataFrame(matriculas_ws.get_all_records(head=1))
        df_matriculas_raw['ID'] = pd.to_numeric(df_matriculas_raw['ID'], errors='coerce')

        row_index_df = df_matriculas_raw[df_matriculas_raw['ID'] == id_aluno].index
        if row_index_df.empty:
            st.error(f"Erro crítico: Não foi possível encontrar o ID {id_aluno} para atualizar.")
            st.stop()

        excel_row_index = row_index_df[0] + 2
        headers = df_matriculas_raw.columns.tolist()
        cells_to_update = []

        for col_nome, novo_valor in dados_para_atualizar.items():
            if col_nome not in headers:
                st.warning(f"A coluna '{col_nome}' não foi encontrada na planilha. Ignorando atualização.")
                continue

            excel_col_index = headers.index(col_nome) + 1
            cells_to_update.append(gspread.Cell(excel_row_index, excel_col_index, str(novo_valor)))

        if cells_to_update:
            matriculas_ws.update_cells(cells_to_update, value_input_option='USER_ENTERED')
            return True
        return False
    except Exception as e:
        st.error(f"Erro ao tentar atualizar a planilha: {e}")
        return False


def atualizar_lancamento_despesa(id_despesa, dados_para_atualizar):
    """Atualiza uma linha específica na aba 'Lancamentos_Despesas' com base no ID."""
    try:
        despesas_ws = sheet.worksheet("Lancamentos_Despesas")
        df_despesas_raw = pd.DataFrame(despesas_ws.get_all_records(head=1))
        df_despesas_raw['ID'] = pd.to_numeric(df_despesas_raw['ID'], errors='coerce')

        row_index_df = df_despesas_raw[df_despesas_raw['ID'] == id_despesa].index
        if row_index_df.empty:
            st.error(f"Erro crítico: Não foi possível encontrar o ID de despesa {id_despesa} para atualizar.")
            st.stop()

        excel_row_index = row_index_df[0] + 2
        headers = df_despesas_raw.columns.tolist()
        cells_to_update = []

        for col_nome, novo_valor in dados_para_atualizar.items():
            if col_nome not in headers:
                st.warning(f"A coluna '{col_nome}' não foi encontrada na planilha. Ignorando atualização.")
                continue

            excel_col_index = headers.index(col_nome) + 1
            cells_to_update.append(gspread.Cell(excel_row_index, excel_col_index, str(novo_valor)))

        if cells_to_update:
            despesas_ws.update_cells(cells_to_update, value_input_option='USER_ENTERED')
            return True
        return False
    except Exception as e:
        st.error(f"Erro ao tentar atualizar a despesa: {e}")
        return False


# -----------------------------------------------------
# PÁGINA: CADASTRAR ALUNO(A)
# -----------------------------------------------------
def pagina_cadastro():
    st.title("Cadastrar Novo(a) Aluno(a)")

    try:
        df_planos = load_planos()
        if df_planos.empty or 'Plano' not in df_planos.columns:
            st.error("Não foi possível carregar a lista de planos.")
            lista_planos = ["Erro ao carregar"]
        else:
            lista_planos = df_planos['Plano'].tolist()
    except Exception as e:
        st.error(f"Erro ao buscar planos: {e}")
        lista_planos = ["Erro ao carregar"]

    with st.form("cadastro_form", clear_on_submit=True):
        st.subheader("Informações Pessoais")
        col1, col2 = st.columns(2)
        with col1:
            nome = st.text_input("Nome Completo*", placeholder="Ex: Maria da Silva")
            cpf_input = st.text_input("CPF*", placeholder="Ex: 123.456.789-00")
            telefone = st.text_input("Telefone/Celular*", placeholder="Ex: (11) 99999-8888")
            email = st.text_input("Email*", placeholder="Ex: maria@email.com")
            data_nascimento = st.date_input("Data de Nascimento", value=None, min_value=datetime(1900, 1, 1),
                                            max_value=datetime.now(), format="YYYY/MM/DD")

        with col2:
            sexo = st.selectbox("Sexo", ["Mulher", "Homem", "Outro", "Não informar"], index=None,
                                placeholder="Selecione...")
            emprego = st.text_input("Profissão/Emprego", placeholder="Ex: Advogada, Estudante...")
            onde_conheceu = st.text_input("Onde nos conheceu?", placeholder="Ex: Instagram, Indicação...")
            cep = st.text_input("CEP", placeholder="Ex: 00000-000")
            endereco = st.text_input("Endereço", placeholder="Ex: Rua das Flores, 123")

        st.divider()
        st.subheader("Informações do Plano")
        col3, col4, col5 = st.columns(3)
        with col3:
            plano_selecionado = st.selectbox("Plano*", lista_planos, index=None, placeholder="Selecione o plano")
        with col4:
            data_inicio = st.date_input("Data de Início*",
                                        help="Para alunos antigos, insira a data da PRIMEIRA matrícula (ex: 10/10/2022). O sistema ajustará para o ciclo atual e salvará o histórico.")
        with col5:
            status = st.selectbox("Status*", ["Ativa", "Inativa", "Cancelada", "Congelado"], index=0)

        st.subheader("Informações de Desconto")
        col6, col7 = st.columns(2)
        with col6:
            desconto_percentual = st.number_input("Desconto (%)", 0.0, 25.0, 0.0, 1.0, "%.1f",
                                                  help="Insira um valor percentual de desconto (máx 25%).")
        with col7:
            justificativa_desconto = st.text_input("Justificativa do Desconto",
                                                   placeholder="Ex: Indicação de Maria, Plano família...")

        notas = st.text_area("Notas / Observações Gerais",
                             placeholder="Alguma observação sobre o(a) aluno(a)? (lesões, etc)")

        submitted = st.form_submit_button("Salvar Novo(a) Aluno(a)")

    if submitted:
        validador_cpf = CPF()
        if not nome or not cpf_input or not plano_selecionado or not telefone or not email:
            st.warning("Por favor, preencha os campos obrigatórios (*).")
            st.stop()

        if not validador_cpf.validate(cpf_input):
            st.error(f"CPF inválido! O número '{cpf_input}' não é um CPF válido. Verifique a digitação.")
            st.stop()

        if desconto_percentual > 0 and not justificativa_desconto:
            st.warning("Por favor, insira a 'Justificativa do Desconto' para aplicar o valor.")
            st.stop()

        try:
            df_matriculas = load_matriculas()

            if not df_matriculas.empty and 'CPF' in df_matriculas.columns:
                if not df_matriculas[df_matriculas['CPF'] == cpf_input].empty:
                    st.error(f"Erro: O CPF '{cpf_input}' já está cadastrado no sistema!")
                    st.stop()
                if 'ID' in df_matriculas.columns and not df_matriculas['ID'].isnull().all():
                    last_id = int(df_matriculas['ID'].max())
                    novo_id = last_id + 1
                else:
                    novo_id = 1
            else:
                novo_id = 1

            data_cadastro = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            data_inicio_original_str = data_inicio.strftime('%d/%m/%Y')
            data_inicio_dt = datetime.combine(data_inicio, dt_time.min)

            ciclos_a_registrar = []
            data_inicio_para_matricula = data_inicio_dt

            try:
                plano_info = df_planos[df_planos['Plano'] == plano_selecionado].iloc[0]
                duracao_meses = int(plano_info['Duracao_Meses'])

                if duracao_meses > 0:
                    ciclos_a_registrar.append(data_inicio_dt)
                    data_fim_ciclo = data_inicio_dt + relativedelta(months=duracao_meses)
                    data_inicio_ciclo_atual = data_inicio_dt

                    if hoje.date() > data_fim_ciclo.date() and status == "Ativa":
                        while hoje.date() > (data_inicio_ciclo_atual + relativedelta(months=duracao_meses)).date():
                            data_inicio_ciclo_atual = data_inicio_ciclo_atual + relativedelta(months=duracao_meses)
                            ciclos_a_registrar.append(data_inicio_ciclo_atual)

                        data_inicio_para_matricula = data_inicio_ciclo_atual

                        st.toast(
                            f"Ajuste de aluno antigo: {len(ciclos_a_registrar)} ciclos detectados. Data de início alterada de {data_inicio_original_str} para {data_inicio_para_matricula.strftime('%d/%m/%Y')} (ciclo atual).",
                            icon="🔔")
                else:
                    ciclos_a_registrar.append(data_inicio_dt)

            except IndexError:
                st.error(
                    f"O plano '{plano_selecionado}' não foi encontrado na aba 'Planos'. Não foi possível calcular a renovação automática.")
                ciclos_a_registrar.append(data_inicio_dt)
            except Exception as e:
                st.warning(f"Não foi possível calcular a renovação automática: {e}")
                ciclos_a_registrar.append(data_inicio_dt)

            data_inicio_str = data_inicio_para_matricula.strftime("%Y-%m-%d")
            data_nascimento_str = data_nascimento.strftime("%Y-%m-%d") if data_nascimento else ""
            data_congelamento_str = ""
            data_primeira_matricula_str = data_inicio.strftime("%Y-%m-%d")

            if status == "Congelado":
                data_congelamento_str = datetime.now().strftime("%Y-%m-%d")
                st.toast("Status 'Congelado' definido. Use a página 'Gerenciar Status' para reativar.")

            nova_linha_matricula = [
                novo_id, data_cadastro, nome, cpf_input, telefone,
                email, plano_selecionado, data_inicio_str, status,
                cep, endereco, data_nascimento_str, onde_conheceu,
                sexo, emprego, notas, desconto_percentual, justificativa_desconto,
                data_congelamento_str,
                data_primeira_matricula_str
            ]

            matriculas_ws = sheet.worksheet("Matriculas")
            matriculas_ws.append_row([str(item) for item in nova_linha_matricula], value_input_option='USER_ENTERED')

            try:
                plano_info_hist = df_planos[df_planos['Plano'] == plano_selecionado].iloc[0]
                preco_plano_hist = plano_info_hist['Preco_Mensal']
                valor_final_contrato = preco_plano_hist * (1 - desconto_percentual / 100)

                df_hist = load_historico_renovacoes()
                id_hist = (df_hist['ID_Historico'].max() + 1) if not df_hist.empty and not df_hist[
                    'ID_Historico'].isnull().all() else 1

                linhas_historico = []
                if not ciclos_a_registrar:
                    ciclos_a_registrar.append(data_inicio_para_matricula)

                for data_ciclo in ciclos_a_registrar:
                    linha_historico = [
                        id_hist, novo_id, nome, plano_selecionado,
                        data_ciclo.strftime("%Y-%m-%d"),
                        valor_final_contrato,
                        data_cadastro
                    ]
                    linhas_historico.append([str(item) for item in linha_historico])
                    id_hist += 1

                hist_ws = sheet.worksheet("Historico_Renovacoes")
                hist_ws.append_rows(linhas_historico, value_input_option='USER_ENTERED')

            except Exception as e_hist:
                st.error(f"Erro ao salvar no histórico de renovações: {e_hist}")

            clear_all_caches()
            st.success(f"Aluno(a) {nome} cadastrado(a) com sucesso! (Matrícula ID: {novo_id})")
            st.balloons()
            time.sleep(2)
            st.rerun()

        except Exception as e:
            st.error(f"Erro ao salvar na planilha: {e}")
            st.error(
                "Verifique se as colunas da aba 'Matriculas' estão na ordem correta (incluindo 'Data_Primeira_Matricula' como a ÚLTIMA coluna).")


# -----------------------------------------------------
# PÁGINA: ALUNOS E HISTÓRICO
# -----------------------------------------------------
def pagina_todos_alunos():
    st.title("Buscar Aluno(a) e Histórico")

    try:
        df_matriculas = load_matriculas()
        df_presencas = load_presencas()

        if df_matriculas.empty:
            st.info("Nenhum(a) aluno(a) cadastrado(a) ainda.")
            st.stop()

        if 'Nome' not in df_matriculas.columns or 'ID' not in df_matriculas.columns:
            st.error("Erro Crítico: A planilha 'Matriculas' precisa das colunas 'Nome' e 'ID'.")
            st.stop()

        df_matriculas_sorted = df_matriculas.sort_values(by="Nome")
        dict_alunas = pd.Series(df_matriculas_sorted.ID.values, index=df_matriculas_sorted.Nome).to_dict()

        modo_busca = st.radio(
            "Selecione o modo de visualização:",
            ["Buscar Aluno(a) Específico(a)", "Ver Lista Completa"],
            index=0,
            horizontal=True
        )
        st.divider()

        if modo_busca == "Buscar Aluno(a) Específico(a)":
            st.header("Busca Individual e Histórico")
            opcoes_alunas = [""] + list(dict_alunas.keys())
            nome_selecionado = st.selectbox(
                "Digite ou selecione o nome do(a) aluno(a):",
                options=opcoes_alunas,
                index=0,
                placeholder="Selecione..."
            )

            if nome_selecionado:
                id_aluno = dict_alunas[nome_selecionado]
                dados_aluno = df_matriculas[df_matriculas['ID'] == id_aluno].iloc[0]

                st.subheader(f"Ficha de {dados_aluno['Nome']}")
                with st.expander("Ver/Ocultar Dados do Cadastro", expanded=False):
                    col1, col2 = st.columns(2)
                    col1.text(f"Status: {dados_aluno.get('Status', 'N/A')}")
                    col1.text(f"Plano: {dados_aluno.get('Plano', 'N/A')}")
                    col2.text(f"CPF: {dados_aluno.get('CPF', 'N/A')}")
                    col2.text(f"Telefone: {dados_aluno.get('Telefone', 'N/A')}")
                    col1.text(f"Email: {dados_aluno.get('Email', 'N/A')}")

                    data_nasc_val = pd.to_datetime(dados_aluno.get('Data_Nascimento'), errors='coerce')
                    data_nasc_str = data_nasc_val.strftime('%d/%m/%Y') if pd.notna(data_nasc_val) else 'N/A'
                    col2.text(f"Nascimento: {data_nasc_str}")
                    col1.text(f"Profissão: {dados_aluno.get('Emprego', 'N/A')}")

                    data_cong_val = pd.to_datetime(dados_aluno.get('Data_Congelamento_Inicio'), errors='coerce')
                    if pd.notna(data_cong_val):
                        data_cong_str = data_cong_val.strftime('%d/%m/%Y')
                        col2.markdown(f"**Congelado Desde:** `{data_cong_str}`")

                    desconto = dados_aluno.get('Desconto_Percentual', 0)
                    if desconto > 0:
                        col1.markdown(f"**Desconto:** `{desconto:.1f}%`")
                        col2.markdown(f"**Justificativa:** `{dados_aluno.get('Justificativa_Desconto', 'N/A')}`")

                    data_inicio_val = dados_aluno.get('Data_Inicio')
                    data_inicio_str = data_inicio_val.strftime('%d/%m/%Y') if pd.notna(data_inicio_val) else 'N/A'

                    data_primeira_val = dados_aluno.get('Data_Primeira_Matricula')
                    if pd.isna(data_primeira_val):
                        data_primeira_val = dados_aluno.get('Data_Inicio')

                    data_primeira_str = data_primeira_val.strftime('%d/%m/%Y') if pd.notna(data_primeira_val) else 'N/A'

                    st.text(f"Data 1ª Matrícula: {data_primeira_str}")
                    st.text(f"Início Ciclo Vigente: {data_inicio_str}")

                    st.text(f"Endereço: {dados_aluno.get('Endereco', 'N/A')}, {dados_aluno.get('CEP', 'N/A')}")
                    st.text(f"Notas: {dados_aluno.get('Notas', 'N/A')}")

                st.subheader(f"Histórico de Aulas e Evolução")
                if 'ID_Aluno' not in df_presencas.columns:
                    st.error("Erro Crítico: Coluna 'ID_Aluno' não encontrada na aba 'Presencas_Evolucao'.")
                    st.stop()

                df_historico = df_presencas[df_presencas['ID_Aluno'] == id_aluno]
                if df_historico.empty:
                    st.info("Nenhum registro de presença encontrado para este(a) aluno(a).")
                else:
                    df_historico = df_historico.sort_values(by="Data_Aula", ascending=False)
                    cols_historico = ['Data_Aula', 'Horario_Inicio', 'Notas_Evolucao']
                    cols_display = [col for col in cols_historico if col in df_historico.columns]
                    df_display = df_historico[cols_display].copy()

                    if 'Data_Aula' in df_display:
                        df_display['Data_Aula'] = df_display['Data_Aula'].dt.strftime('%d/%m/%Y')

                    df_display.rename(columns={'Data_Aula': 'Data da Aula', 'Horario_Inicio': 'Horário',
                                               'Notas_Evolucao': 'Notas da Aula (Evolução)'}, inplace=True)
                    st.dataframe(df_display, use_container_width=True, hide_index=True)

        else:  # Modo "Ver Lista Completa"
            st.header("Lista Completa de Alunos(as)")
            if 'Status' in df_matriculas.columns:
                filtro_status = st.radio("Filtrar por Status:",
                                         ["Todos(as)", "Ativa", "Congelado", "Inativa", "Cancelada"],
                                         horizontal=True, index=1)
                if filtro_status == "Todos(as)":
                    df_filtrado = df_matriculas
                else:
                    df_filtrado = df_matriculas[df_matriculas['Status'].str.lower() == filtro_status.lower()]
            else:
                st.warning("Coluna 'Status' não encontrada. Exibindo todos os alunos.")
                df_filtrado = df_matriculas

            st.info(f"Total de Alunos(as) Encontrados(as): {len(df_filtrado)}")
            df_filtrado = df_filtrado.sort_values(by="ID", ascending=False)

            cols_to_show = ['ID', 'Nome', 'Email', 'Telefone', 'Plano', 'Status',
                            'Data_Primeira_Matricula', 'Data_Inicio',
                            'Data_Congelamento_Inicio', 'Data_Nascimento', 'Emprego', 'Desconto_Percentual']
            cols_existentes = [col for col in cols_to_show if col in df_filtrado.columns]
            st.dataframe(df_filtrado[cols_existentes], use_container_width=True)

    except KeyError as e:
        st.error(f"Erro Crítico de Coluna: A coluna {e} não foi encontrada na planilha.")
    except Exception as e:
        st.error(f"Erro ao buscar alunos(as): {e}")


# -----------------------------------------------------
# PÁGINA: LANÇAR DESPESA
# -----------------------------------------------------
def pagina_lancar_despesa():
    st.title("Lançar Nova Despesa (Contas a Pagar)")
    st.write("Use esta página para provisionar *todas* as contas (fixas, variáveis, pontuais).")

    with st.form("despesa_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            descricao = st.text_input("Descrição da Despesa*", placeholder="Ex: Aluguel, Conta de Luz, Reforma")
            valor_total = st.number_input("Valor Total (R$)*", min_value=0.01, format="%.2f",
                                          help="Para recorrentes, insira o valor de UMA parcela.")
            tipo = st.selectbox("Tipo de Despesa*", ["Fixo", "Variável", "Pontual"], placeholder="Selecione...")

        with col2:
            data_inicio_competencia = st.date_input("Data de Início (Primeira competência)*", value=datetime.now())
            recorrente = st.checkbox("É uma despesa recorrente (mensal)?",
                                     help="Ex: Aluguel, Salário. Marque isso para lançar 12 parcelas.")

            if recorrente:
                num_parcelas = 12
            else:
                num_parcelas = st.number_input("Número de Parcelas*", min_value=1, value=1, step=1,
                                               help="Para contas únicas, deixe 1. Para compras parceladas, mude.")

        data_vencimento = st.date_input("Data de Vencimento (1ª Parcela)", value=None)
        submitted = st.form_submit_button("Lançar Despesa(s)")

    if submitted:
        if not descricao or not valor_total or not tipo:
            st.warning("Por favor, preencha todos os campos obrigatórios (*).")
            st.stop()
        try:
            df_despesas = load_despesas()
            if df_despesas.empty or 'ID' not in df_despesas.columns or df_despesas['ID'].isnull().all():
                proximo_id = 1
            else:
                proximo_id = int(df_despesas['ID'].max()) + 1

            valor_parcela = valor_total / num_parcelas if not recorrente else valor_total
            data_cadastro = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            linhas_a_adicionar = []
            recorrente_str = "Sim" if recorrente else "Não"

            for i in range(num_parcelas):
                data_competencia_parcela = data_inicio_competencia + relativedelta(months=i)
                mes_competencia = data_competencia_parcela.month
                ano_competencia = data_competencia_parcela.year

                descricao_parcela = descricao
                if num_parcelas > 1 and not recorrente:
                    descricao_parcela = f"{descricao} ({i + 1}/{num_parcelas})"
                elif recorrente:
                    descricao_parcela = f"{descricao} (Recorrente)"

                vencimento_parcela_str = ""
                if data_vencimento:
                    vencimento_parcela = data_vencimento + relativedelta(months=i)
                    vencimento_parcela_str = vencimento_parcela.strftime("%Y-%m-%d")

                nova_linha = [
                    proximo_id, data_cadastro, descricao_parcela, valor_parcela,
                    mes_competencia, ano_competencia, tipo, "Pendente",
                    "", 0.0, "", recorrente_str, vencimento_parcela_str
                ]
                linhas_a_adicionar.append(nova_linha)
                proximo_id += 1

            despesas_ws = sheet.worksheet("Lancamentos_Despesas")
            despesas_ws.append_rows(linhas_a_adicionar, value_input_option='USER_ENTERED')
            clear_all_caches()

            st.success(f"Despesa '{descricao}' lançada com sucesso em {num_parcelas} parcela(s)!")
            st.balloons()
            time.sleep(2)
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao salvar despesa: {e}")
            st.error("Verifique se as 13 colunas da aba 'Lancamentos_Despesas' estão na ordem correta.")


# -----------------------------------------------------
# PÁGINA: REGISTRAR PRESENÇA
# -----------------------------------------------------
def pagina_presenca():
    st.title("Registrar Presença e Evolução da Aula")

    try:
        df_matriculas = load_matriculas()
        df_ativas = df_matriculas[df_matriculas['Status'].str.lower() == 'ativa']
        if df_ativas.empty:
            st.warning("Nenhuma aluna 'Ativa' encontrada para registrar a presença.")
            st.stop()
        dict_alunas = pd.Series(df_ativas.ID.values, index=df_ativas.Nome).to_dict()
    except Exception as e:
        st.error(f"Erro ao carregar lista de alunas: {e}")
        st.stop()

    with st.form("presenca_form", clear_on_submit=True):
        nome_selecionado = st.selectbox("Aluno(a)*", options=dict_alunas.keys(), index=None,
                                        placeholder="Selecione a aluna...")
        col1, col2 = st.columns(2)
        with col1:
            data_aula = st.date_input("Data da Aula*", value=datetime.now())
        with col2:
            horario_inicio = st.time_input("Horário da Aula*", value=dt_time(8, 0))
        notas = st.text_area("Anotações da Aula / Evolução da Aluna",
                             placeholder="Ex: Conseguiu fazer a série 'Stomach Massage' completa...")
        submitted = st.form_submit_button("Registrar Presença e Salvar Notas")

    if submitted:
        if not nome_selecionado:
            st.warning("Por favor, selecione a aluna.")
            st.stop()

        horario_minimo = dt_time(6, 0)
        horario_maximo = dt_time(21, 0)
        if not (horario_minimo <= horario_inicio <= horario_maximo):
            st.error(f"Horário inválido! Insira um horário entre {horario_minimo:%H:%M} e {horario_maximo:%H:%M}.")
            st.stop()

        try:
            id_aluna_selecionada = dict_alunas[nome_selecionado]
            df_presencas = load_presencas()
            if df_presencas.empty or 'ID_Presenca' not in df_presencas.columns or df_presencas[
                'ID_Presenca'].isnull().all():
                novo_id_presenca = 1
            else:
                novo_id_presenca = int(df_presencas['ID_Presenca'].max()) + 1

            nova_linha = [
                novo_id_presenca, id_aluna_selecionada, nome_selecionado,
                data_aula.strftime("%Y-%m-%d"), horario_inicio.strftime("%H:%M:%S"), notas
            ]
            presencas_ws = sheet.worksheet("Presencas_Evolucao")
            presencas_ws.append_row([str(item) for item in nova_linha], value_input_option='USER_ENTERED')

            clear_all_caches()
            st.success(f"Presença e notas da aluna {nome_selecionado} salvas com sucesso!")
            st.balloons()
            time.sleep(2)
            st.rerun()

        except Exception as e:
            st.error(f"Erro ao salvar presença: {e}")


# -----------------------------------------------------
# === PÁGINA: LANÇAR PAGAMENTO (CORRIGIDA DEFINITIVA) ===
# -----------------------------------------------------
def pagina_lancar_pagamento():
    st.title("💰 Lançar Pagamento Recebido")
    try:
        df_matriculas = load_matriculas()
        df_taxas = load_taxas()  # Carrega a nova tabela de taxas

        df_ativas = df_matriculas[df_matriculas['Status'].str.lower() == 'ativa']
        if df_ativas.empty:
            st.warning("Nenhuma aluna 'Ativa' encontrada para lançar pagamento.")
            st.stop()
        if df_taxas.empty:
            st.error("Tabela 'Config_Taxas' não encontrada ou vazia. Não é possível calcular taxas.")
            st.stop()

        dict_alunas = pd.Series(df_ativas.ID.values, index=df_ativas.Nome).to_dict()

    except gspread.exceptions.WorksheetNotFound:
        st.error("Erro Crítico: Aba 'Config_Taxas' não foi encontrada. Crie-a conforme as instruções.")
        st.stop()
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        st.stop()

    with st.expander("🕵️ Raio-X da Tabela de Taxas (Debug)"):
        st.dataframe(df_taxas)

    # Inicializa variáveis
    valor_liquido_calculado = 0.0
    taxa_percentual = 0.0

    # --- Widgets fora do form para permitir interatividade ---
    col1, col2 = st.columns(2)

    with col1:
        nome_selecionado = st.selectbox("Aluno(a)*", options=dict_alunas.keys(), index=None,
                                        placeholder="Selecione a aluna...")
        valor_pago_bruto = st.number_input("Valor Pago (Bruto) (R$)*", min_value=0.01, format="%.2f", value=None)
        mes_competencia = st.selectbox(
            "Mês de Competência*",
            options=LISTA_MESES_NOMES.keys(),
            format_func=lambda mes: f"{mes} - {LISTA_MESES_NOMES[mes]}",
            index=MES_ATUAL - 1
        )

    with col2:
        data_pagamento = st.date_input("Data do Pagamento*", value=datetime.now())
        ano_competencia = st.number_input("Ano de Competência*", min_value=2024, value=ANO_ATUAL, step=1)

    st.divider()
    st.subheader("Forma de Pagamento (Cálculo de Taxa)")

    # --- SELETORES DINÂMICOS (CORRIGIDOS COM CHAVES ÚNICAS) ---
    col_taxa1, col_taxa2, col_taxa3 = st.columns(3)

    # --- Coluna 1: Bandeira ---
    with col_taxa1:
        opcoes_bandeira = df_taxas['Bandeira'].unique()
        bandeira_selecionada = st.selectbox("Bandeira*", options=opcoes_bandeira, index=None,
                                            placeholder="Selecione...")

    # --- Coluna 2: Tipo (Depende da Bandeira) ---
    if bandeira_selecionada:
        opcoes_tipo = df_taxas[df_taxas['Bandeira'] == bandeira_selecionada]['Tipo'].unique()
        with col_taxa2:
            # key dinâmica força o reset se a bandeira mudar
            tipo_selecionado = st.selectbox("Tipo*", options=opcoes_tipo, index=None,
                                            placeholder="Selecione...", key=f"tipo_{bandeira_selecionada}")
    else:
        with col_taxa2:
            tipo_selecionado = st.selectbox("Tipo*", options=[], index=None, placeholder="Selecione...",
                                            disabled=True, key="tipo_disabled")

    # --- Coluna 3: Parcela (Depende da Bandeira E Tipo) ---
    if bandeira_selecionada and tipo_selecionado:
        opcoes_parcela = df_taxas[
            (df_taxas['Bandeira'] == bandeira_selecionada) &
            (df_taxas['Tipo'] == tipo_selecionado)
            ]['Parcela'].unique()
        with col_taxa3:
            # key dinâmica força o reset se a bandeira ou tipo mudarem
            parcela_selecionada = st.selectbox("Parcela*", options=opcoes_parcela, index=0,
                                               key=f"parc_{bandeira_selecionada}_{tipo_selecionado}")
    else:
        with col_taxa3:
            parcela_selecionada = st.selectbox("Parcela*", options=[], index=None, placeholder="Selecione...",
                                               disabled=True, key="parc_disabled")

    # --- CÁLCULO AUTOMÁTICO ---
    valor_bruto_calculo = valor_pago_bruto if valor_pago_bruto else 0.0
    valor_liquido_calculado = valor_bruto_calculo

    if valor_bruto_calculo > 0 and bandeira_selecionada and tipo_selecionado and parcela_selecionada:
        try:
            taxa_row = df_taxas[
                (df_taxas['Bandeira'] == bandeira_selecionada) &
                (df_taxas['Tipo'] == tipo_selecionado) &
                (df_taxas['Parcela'] == parcela_selecionada)
                ]
            if not taxa_row.empty:
                taxa_percentual = taxa_row['Taxa'].iloc[0]
                valor_taxa = valor_bruto_calculo * taxa_percentual
                valor_liquido_calculado = valor_bruto_calculo - valor_taxa

                st.info(
                    f"Taxa Aplicada: {taxa_percentual:.2%} (R$ {valor_taxa:,.2f})  |  **Valor Líquido: R$ {valor_liquido_calculado:,.2f}**")
            else:
                st.error("Combinação de taxa não encontrada! Verifique a planilha 'Config_Taxas'.")
        except Exception as e:
            st.error(f"Erro ao calcular taxa: {e}")
    # --- FIM DO CÁLCULO ---

    notas = st.text_area("Notas / Observações", placeholder="Ex: Pagamento referente a 50% da mensalidade.")

    # --- Botão normal, não um submit de form ---
    submitted = st.button("Lançar Pagamento")

    if submitted:
        # Validação final dos dados
        if not nome_selecionado or not valor_pago_bruto or not bandeira_selecionada or not tipo_selecionado or not parcela_selecionada:
            st.warning("Por favor, preencha todos os campos obrigatórios (*).")
            st.stop()

        # Recalcula os valores finais para garantir que estão corretos
        try:
            taxa_row = df_taxas[
                (df_taxas['Bandeira'] == bandeira_selecionada) &
                (df_taxas['Tipo'] == tipo_selecionado) &
                (df_taxas['Parcela'] == parcela_selecionada)
                ]
            if taxa_row.empty:
                st.error("Erro ao salvar: Combinação de taxa não encontrada. Verifique 'Config_Taxas'.")
                st.stop()

            taxa_percentual_final = taxa_row['Taxa'].iloc[0]
            valor_taxa_final = valor_pago_bruto * taxa_percentual_final
            valor_liquido_final = valor_pago_bruto - valor_taxa_final
        except Exception:
            st.error("Erro ao salvar. Combinação de taxa inválida.")
            st.stop()

        # Constrói o nome da forma de pagamento
        forma_pagamento_str = f"{bandeira_selecionada}"
        if tipo_selecionado != 'N/A':
            forma_pagamento_str += f" - {tipo_selecionado}"
        if parcela_selecionada != 'N/A' and parcela_selecionada != '1x':
            forma_pagamento_str += f" {parcela_selecionada}"

        try:
            id_aluna_selecionada = dict_alunas[nome_selecionado]
            df_pagamentos = load_pagamentos()

            if df_pagamentos.empty or 'ID_Pagamento' not in df_pagamentos.columns or df_pagamentos[
                'ID_Pagamento'].isnull().all():
                novo_id_pagamento = 1
            else:
                novo_id_pagamento = int(df_pagamentos['ID_Pagamento'].max()) + 1

            nova_linha = [
                novo_id_pagamento, id_aluna_selecionada, nome_selecionado,
                data_pagamento.strftime("%Y-%m-%d"), mes_competencia, ano_competencia,
                valor_pago_bruto,  # Coluna G (Bruto)
                forma_pagamento_str,  # Coluna H
                notas,  # Coluna I
                valor_liquido_final  # Coluna J (Líquido)
            ]
            pagamentos_ws = sheet.worksheet("Pagamentos_Recebidos")
            pagamentos_ws.append_row([str(item) for item in nova_linha], value_input_option='USER_ENTERED')

            clear_all_caches()
            st.success(
                f"Pagamento (Bruto: R$ {valor_pago_bruto:,.2f} | Líquido: R$ {valor_liquido_final:,.2f}) para {nome_selecionado} lançado com sucesso!")
            st.balloons()
            time.sleep(2)
            st.rerun()  # Rerun para limpar os campos
        except Exception as e:
            st.error(f"Erro ao salvar pagamento: {e}")
            st.error("Verifique se a aba 'Pagamentos_Recebidos' tem 10 colunas (terminando em 'Valor_Liquido').")


# -----------------------------------------------------
# PÁGINA: PAGAR CONTAS (BAIXA)
# -----------------------------------------------------
def pagina_contas_a_pagar():
    st.title("🧾 Pagar Contas (Baixa de Despesas)")
    st.write("Aqui você confirma o pagamento das contas provisionadas.")

    try:
        df_despesas = load_despesas()
        if df_despesas.empty:
            st.info("Nenhuma despesa lançada no sistema.")
            st.stop()

        st.subheader("Filtrar Contas a Pagar")
        status_filtro_opcoes = ["Pendente", "Parcial", "Pago"]
        status_selecionado = st.multiselect("Filtrar por Status", options=status_filtro_opcoes,
                                            default=["Pendente", "Parcial"])

        if not status_selecionado:
            df_filtrado_status = df_despesas
        else:
            df_filtrado_status = df_despesas[df_despesas['Status_Pagamento'].isin(status_selecionado)]

        col1, col2 = st.columns(2)
        with col1:
            mes_selecionado = st.selectbox("Mês de Competência", options=LISTA_MESES_NOMES.keys(),
                                           format_func=lambda mes: f"{mes} - {LISTA_MESES_NOMES[mes]}",
                                           index=MES_ATUAL - 1)
        with col2:
            ano_selecionado = st.number_input("Ano de Competência", min_value=2024, value=ANO_ATUAL, step=1)

        df_contas_mes = df_filtrado_status[
            (df_filtrado_status['Mes_Competencia'] == mes_selecionado) &
            (df_filtrado_status['Ano_Competencia'] == ano_selecionado)
            ].sort_values(by="ID", ascending=False)

        if df_contas_mes.empty:
            st.info(
                f"Nenhuma conta encontrada com o status '{', '.join(status_selecionado)}' para {LISTA_MESES_NOMES[mes_selecionado]}/{ano_selecionado}.")
            st.stop()

        st.info(f"Exibindo {len(df_contas_mes)} conta(s).")

        for _, conta in df_contas_mes.iterrows():
            id_despesa = conta['ID']
            valor_previsto = conta['Valor']
            valor_pago = conta['Valor_Pago']
            saldo_devedor = valor_previsto - valor_pago

            st.divider()
            col1, col2, col3 = st.columns(3)
            col1.markdown(f"**{conta['Descricao']}** (ID: {id_despesa})")
            col2.metric("Previsto", f"R$ {valor_previsto:,.2f}")

            if conta['Status_Pagamento'] == "Pendente":
                col3.metric("A Pagar", f"R$ {saldo_devedor:,.2f}", delta_color="inverse")
            elif conta['Status_Pagamento'] == "Parcial":
                col3.metric("A Pagar", f"R$ {saldo_devedor:,.2f}", delta=f"Pago: R$ {valor_pago:,.2f}",
                            delta_color="inverse")
            else:
                col3.metric("Pago", f"R$ {valor_pago:,.2f}", delta_color="normal")

            if conta['Status_Pagamento'] != "Pago":
                with st.expander("Pagar/Registrar Baixa desta Conta"):
                    with st.form(f"form_pagar_despesa_{id_despesa}", clear_on_submit=True):
                        st.write(f"Pagando: {conta['Descricao']}")

                        col_form_1, col_form_2 = st.columns(2)
                        with col_form_1:
                            data_pagamento = st.date_input("Data do Pagamento*", value=datetime.now())
                            forma_pagamento = st.selectbox("Forma de Pagamento*",
                                                           ["PIX", "Boleto", "Cartão de Débito", "Dinheiro",
                                                            "Cartão de Crédito", "Transferência"],
                                                           index=None, placeholder="Selecione...")
                        with col_form_2:
                            valor_a_pagar = st.number_input("Valor Pago*", min_value=0.01, value=float(saldo_devedor),
                                                            format="%.2f")

                        submitted_pagar = st.form_submit_button("Confirmar Pagamento")

                    if submitted_pagar:
                        if not data_pagamento or not forma_pagamento or not valor_a_pagar:
                            st.warning("Preencha todos os campos do pagamento.")
                            st.stop()

                        novo_valor_pago_total = valor_pago + valor_a_pagar
                        novo_status = "Pago"

                        if novo_valor_pago_total < valor_previsto:
                            novo_status = "Parcial"
                        elif novo_valor_pago_total > valor_previsto:
                            st.warning(
                                f"O valor pago (R$ {novo_valor_pago_total:,.2f}) é maior que o previsto (R$ {valor_previsto:,.2f}). Registrando como 'Pago'.")

                        dados_baixa = {
                            "Status_Pagamento": novo_status,
                            "Data_Pagamento": data_pagamento.strftime("%Y-%m-%d"),
                            "Valor_Pago": novo_valor_pago_total,
                            "Forma_Pagamento": forma_pagamento
                        }

                        if atualizar_lancamento_despesa(id_despesa, dados_baixa):
                            st.success(f"Pagamento da despesa '{conta['Descricao']}' registrado com sucesso!")
                            clear_all_caches()
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Falha ao registrar o pagamento.")
    except Exception as e:
        st.exception(f"Ocorreu um erro inesperado ao carregar as contas a pagar: {e}")


# -----------------------------------------------------
# === PÁGINA: DASHBOARD FINANCEIRO (REFORMULADA - ETAPA 2) ===
# -----------------------------------------------------
def pagina_financeiro():
    st.title("📈 Dashboard Financeiro")

    try:
        df_planos = load_planos()
        df_matriculas = load_matriculas()
        df_despesas = load_despesas()
        df_pagamentos = load_pagamentos()

        st.subheader("Análise de Fluxo de Caixa (Realizado vs. Previsto)")
        col1, col2 = st.columns(2)
        with col1:
            mes_selecionado = st.selectbox("Mês de Competência", options=LISTA_MESES_NOMES.keys(),
                                           format_func=lambda mes: f"{mes} - {LISTA_MESES_NOMES[mes]}",
                                           index=MES_ATUAL - 1)
        with col2:
            ano_selecionado = st.number_input("Ano de Competência", min_value=2024, value=ANO_ATUAL, step=1)

        data_filtro_inicio = datetime(ano_selecionado, mes_selecionado, 1)
        ultimo_dia = calendar.monthrange(ano_selecionado, mes_selecionado)[1]
        data_filtro_fim = datetime(ano_selecionado, mes_selecionado, ultimo_dia)

        # Cálculo de Receita Prevista
        total_receita_prevista = 0
        total_descontado_mes = 0
        df_receita_prevista_mes = pd.DataFrame()
        if not df_planos.empty and not df_matriculas.empty:
            df_ativas = df_matriculas[df_matriculas.get('Status', pd.Series(dtype=str)).str.lower() == 'ativa']
            df_receita_join = df_ativas.merge(df_planos, left_on='Plano', right_on='Plano')

            if 'Data_Inicio' in df_receita_join.columns and 'Duracao_Meses' in df_receita_join.columns:
                df_receita_join['Data_Fim'] = df_receita_join.apply(
                    lambda row: row['Data_Inicio'] + pd.DateOffset(months=int(row['Duracao_Meses']))
                    if pd.notna(row['Data_Inicio']) else pd.NaT, axis=1)
                df_receita_join = df_receita_join.dropna(subset=['Data_Inicio', 'Data_Fim'])

                df_receita_prevista_mes = df_receita_join[
                    (df_receita_join['Data_Inicio'] <= data_filtro_fim) &
                    (df_receita_join['Data_Fim'] > data_filtro_inicio)]

                if not df_receita_prevista_mes.empty:
                    preco_col = df_receita_prevista_mes.get('Preco_Mensal', 0)
                    desc_col = df_receita_prevista_mes.get('Desconto_Percentual', 0)
                    df_receita_prevista_mes['Valor_Plano_Final'] = preco_col * (1 - desc_col / 100)
                    df_receita_prevista_mes['Valor_Descontado'] = preco_col - df_receita_prevista_mes[
                        'Valor_Plano_Final']
                    total_receita_prevista = df_receita_prevista_mes['Valor_Plano_Final'].sum()
                    total_descontado_mes = df_receita_prevista_mes['Valor_Descontado'].sum()

        # --- CÁLCULO DE RECEITA REALIZADA (ATUALIZADO) ---
        total_receita_bruta_realizada = 0
        total_receita_liquida_realizada = 0
        total_taxas = 0
        df_pagamentos_mes_filtrado = pd.DataFrame()
        if not df_pagamentos.empty:
            mes_selecionado_int = int(mes_selecionado)
            ano_selecionado_int = int(ano_selecionado)
            df_pagamentos_mes_filtrado = df_pagamentos[
                (df_pagamentos['Mes_Competencia'] == mes_selecionado_int) &
                (df_pagamentos['Ano_Competencia'] == ano_selecionado_int)]

            if 'Valor_Pago' in df_pagamentos_mes_filtrado.columns:
                total_receita_bruta_realizada = df_pagamentos_mes_filtrado['Valor_Pago'].sum()

            if 'Valor_Liquido' in df_pagamentos_mes_filtrado.columns:
                total_receita_liquida_realizada = df_pagamentos_mes_filtrado['Valor_Liquido'].sum()
            else:
                # Fallback se a coluna ainda não foi lida
                total_receita_liquida_realizada = total_receita_bruta_realizada

        total_taxas = total_receita_bruta_realizada - total_receita_liquida_realizada

        # Cálculo de Gastos (Previsto vs Realizado)
        total_gastos_previstos = 0
        total_gastos_realizados = 0
        df_despesas_mes = pd.DataFrame()
        if not df_despesas.empty:
            df_despesas_mes = df_despesas[
                (df_despesas['Mes_Competencia'] == mes_selecionado) &
                (df_despesas['Ano_Competencia'] == ano_selecionado)]
            if 'Valor' in df_despesas_mes.columns:
                total_gastos_previstos = df_despesas_mes['Valor'].sum()
            if 'Valor_Pago' in df_despesas_mes.columns:
                total_gastos_realizados = df_despesas_mes['Valor_Pago'].sum()

        total_gastos_pendentes = total_gastos_previstos - total_gastos_realizados

        # --- CÁLCULO DAS MÉTRICAS FINAIS (ATUALIZADO) ---
        # Saldo a receber é (Previsto - Bruto Pago)
        total_a_receber = total_receita_prevista - total_receita_bruta_realizada
        # Lucro do caixa é (Líquido Recebido - Gastos Pagos)
        lucro_real_caixa = total_receita_liquida_realizada - total_gastos_realizados

        # Exibição (Métricas)
        st.subheader(f"Visão Geral para: {LISTA_MESES_NOMES[mes_selecionado]}/{ano_selecionado}")

        st.markdown("#### 💰 Receitas (Contas a Receber)")
        col1, col2, col3 = st.columns(3)
        col1.metric("Receita Prevista (com desc.)", f"R$ {total_receita_prevista:,.2f}")
        # Esta métrica agora usa o LÍQUIDO, que é o valor real do caixa
        col2.metric("Receita Realizada (Caixa/Líquido)", f"R$ {total_receita_liquida_realizada:,.2f}",
                    help="Este é o valor líquido que entrou na conta, já descontadas as taxas.")
        # Esta métrica continua usando o BRUTO, pois a dívida é bruta
        col3.metric("Saldo a Receber (Devedor)", f"R$ {total_a_receber:,.2f}",
                    delta=f"R$ {-total_a_receber:,.2f}" if total_a_receber > 0 else f"R$ {abs(total_a_receber):,.2f}",
                    delta_color="inverse" if total_a_receber > 0 else "normal",
                    help="Valor Previsto - Valor Bruto Pago (sem taxas).")

        st.markdown("#### 💸 Despesas (Contas a Pagar)")
        col4, col5, col6 = st.columns(3)
        col4.metric("Gastos Previstos (Mês)", f"R$ {total_gastos_previstos:,.2f}", delta_color="inverse")
        col5.metric("Gastos Realizados (Caixa)", f"R$ {total_gastos_realizados:,.2f}", delta_color="inverse")
        col6.metric("Saldo a Pagar (Pendente)", f"R$ {total_gastos_pendentes:,.2f}",
                    delta=f"R$ {-total_gastos_pendentes:,.2f}" if total_gastos_pendentes > 0 else "R$ 0,00",
                    delta_color="inverse" if total_gastos_pendentes > 0 else "normal")

        st.divider()
        st.subheader("Resultado do Mês (Fluxo de Caixa)")

        col7, col8, col9 = st.columns(3)
        # Esta métrica agora usa o LÍQUIDO
        col7.metric("Lucro/Prejuízo Real (Caixa)", f"R$ {lucro_real_caixa:,.2f}",
                    delta_color="normal" if lucro_real_caixa >= 0 else "inverse",
                    help="Receita LÍQUIDA Realizada - Gastos Realizados")

        col8.metric("Total Concedido em Descontos", f"R$ {total_descontado_mes:,.2f}",
                    delta_color="inverse",
                    help="Valor total que o estúdio 'deixou de ganhar' devido aos descontos concedidos (não inclui taxas de cartão).")

        # --- NOVA MÉTRICA DE TAXAS ---
        col9.metric("Total Pago em Taxas (Cartão)", f"R$ {total_taxas:,.2f}",
                    delta_color="inverse" if total_taxas > 0 else "off",
                    help="Diferença entre o Valor Bruto pago pelos alunos e o Valor Líquido que entrou na conta.")

        # --- NOVA SEÇÃO: BRUTO vs LÍQUIDO ---
        st.subheader("Análise de Receita (Bruto vs. Líquido)")
        col10, col11, col12 = st.columns(3)
        col10.metric("Receita Bruta Realizada", f"R$ {total_receita_bruta_realizada:,.2f}",
                     help="Total pago pelos alunos (antes das taxas).")
        col11.metric("Total de Taxas (Maquininha)", f"R$ {total_taxas:,.2f}",
                     delta_color="inverse" if total_taxas > 0 else "off")
        col12.metric("Receita Líquida (Caixa)", f"R$ {total_receita_liquida_realizada:,.2f}",
                     help="Total que de fato entrou na conta.")

        # Gráfico de Fluxo de Caixa (Atualizado para usar LÍQUIDO)
        st.subheader("Comparativo Mensal: Previsto vs. Realizado")
        df_fluxo_mes = pd.DataFrame({
            'Tipo': ['Receita Prevista', 'Receita Realizada (Líquida)', 'Gasto Previsto', 'Gasto Realizado'],
            'Valor': [total_receita_prevista, total_receita_liquida_realizada, total_gastos_previstos,
                      total_gastos_realizados],
            'Categoria': ['Receita', 'Receita', 'Gasto', 'Gasto']
        })
        chart_fluxo_base = alt.Chart(df_fluxo_mes).encode(
            x=alt.X('Tipo:N', title=None, axis=None),
            y=alt.Y('Valor:Q', title='Valor (R$)'),
            color=alt.Color('Tipo:N', legend=alt.Legend(title="Fluxo"),
                            scale=alt.Scale(
                                domain=['Receita Prevista', 'Receita Realizada (Líquida)', 'Gasto Previsto',
                                        'Gasto Realizado'],
                                range=['#1f77b4', '#2ca02c', '#ff7f0e', '#d62728']))
        ).properties(height=350)
        text = chart_fluxo_base.mark_text(dy=-8, color='black').encode(text=alt.Text('Valor:Q', format=",.2f"))
        chart_fluxo_final = chart_fluxo_base.mark_bar() + text
        st.altair_chart(chart_fluxo_final, use_container_width=True)

        st.divider()

        # Tabela de Devedores (Receitas) - (Inalterada, usa Bruto, o que está correto)
        st.subheader(f"Status de Pagamentos de Alunos ({LISTA_MESES_NOMES[mes_selecionado]}/{ano_selecionado})")
        if df_receita_prevista_mes.empty:
            st.info("Nenhuma aluna ativa encontrada para este mês de competência.")
        else:
            df_status_cols = ['ID', 'Nome', 'Plano', 'Preco_Mensal', 'Desconto_Percentual', 'Valor_Plano_Final',
                              'Justificativa_Desconto']
            df_status_cols_exist = [col for col in df_status_cols if col in df_receita_prevista_mes.columns]
            df_status = df_receita_prevista_mes[df_status_cols_exist].copy()
            df_status.rename(
                columns={'ID': 'ID_Aluno', 'Preco_Mensal': 'Valor_Cheio', 'Valor_Plano_Final': 'Valor_Plano_com_Desc'},
                inplace=True)
            df_status = df_status.drop_duplicates(subset=['ID_Aluno'])

            if not df_pagamentos_mes_filtrado.empty and 'ID_Aluno' in df_pagamentos_mes_filtrado.columns:
                # Agrupa pelo Valor_Pago (Bruto) para abater a dívida
                df_pagos_agrupado = df_pagamentos_mes_filtrado.groupby('ID_Aluno')['Valor_Pago'].sum().reset_index()
                df_status = pd.merge(df_status, df_pagos_agrupado, on='ID_Aluno', how='left')
            else:
                df_status['Valor_Pago'] = 0

            df_status['Valor_Pago'] = df_status['Valor_Pago'].fillna(0)
            if 'Valor_Plano_com_Desc' not in df_status.columns:
                df_status['Valor_Plano_com_Desc'] = df_status.get('Valor_Cheio', 0)

            # Saldo Devedor é (Previsto com Desconto - Pago Bruto)
            df_status['Saldo_Devedor'] = df_status['Valor_Plano_com_Desc'] - df_status['Valor_Pago']

            def get_status(row):
                if row['Saldo_Devedor'] <= 0.01: return "Pago"
                if row['Saldo_Devedor'] < row.get('Valor_Plano_com_Desc', 0): return "Parcial"
                return "Não Pago"

            df_status['Status_Pagamento'] = df_status.apply(get_status, axis=1)
            cols_display_status = ['Nome', 'Plano', 'Valor_Cheio', 'Desconto_Percentual', 'Valor_Plano_com_Desc',
                                   'Valor_Pago', 'Saldo_Devedor', 'Status_Pagamento']
            cols_status_exist = [col for col in cols_display_status if col in df_status.columns]
            df_status_display = df_status[cols_status_exist].copy()

            for col in ['Valor_Cheio', 'Valor_Plano_com_Desc', 'Valor_Pago', 'Saldo_Devedor']:
                if col in df_status_display:
                    df_status_display[col] = df_status_display[col].map("R$ {:,.2f}".format)
            if 'Desconto_Percentual' in df_status_display:
                df_status_display['Desconto_Percentual'] = df_status_display['Desconto_Percentual'].map(
                    "{:,.1f}%".format)

            df_status_display = df_status_display.sort_values(by="Nome", ascending=True)
            st.dataframe(df_status_display.rename(
                columns={'Valor_Cheio': 'Plano (Valor Cheio)', 'Desconto_Percentual': 'Desc. (%)',
                         'Valor_Plano_com_Desc': 'A Pagar (com Desc.)',
                         'Valor_Pago': 'Valor Pago (Bruto)'}), use_container_width=True)

        # Relatório de Descontos
        st.subheader("Relatório de Descontos de Alunos do Mês")
        with st.expander("Clique para ver os detalhes de descontos aplicados este mês"):
            if 'Valor_Descontado' in df_receita_prevista_mes.columns:
                df_descontos_mes = df_receita_prevista_mes[df_receita_prevista_mes['Valor_Descontado'] > 0].copy()
            else:
                df_descontos_mes = pd.DataFrame()

            if df_descontos_mes.empty:
                st.info("Nenhum desconto aplicado para alunas ativas este mês.")
            else:
                cols_desc_report = ['Nome', 'Plano', 'Preco_Mensal', 'Desconto_Percentual', 'Valor_Descontado',
                                    'Justificativa_Desconto']
                cols_desc_exist = [col for col in cols_desc_report if col in df_descontos_mes.columns]
                df_descontos_display = df_descontos_mes[cols_desc_exist]

                for col in ['Preco_Mensal', 'Valor_Descontado']:
                    if col in df_descontos_display:
                        df_descontos_display[col] = df_descontos_display[col].map("R$ {:,.2f}".format)
                if 'Desconto_Percentual' in df_descontos_display:
                    df_descontos_display['Desconto_Percentual'] = df_descontos_display['Desconto_Percentual'].map(
                        "{:,.1f}%".format)

                st.dataframe(df_descontos_display.rename(
                    columns={'Preco_Mensal': 'Plano (Valor Cheio)', 'Desconto_Percentual': 'Desc. (%)',
                             'Valor_Descontado': 'Valor (R$)'}), use_container_width=True)

        # Gráficos de Composição e Anual
        st.divider()
        with st.expander("Ver Gráficos de Composição e Projeção Anual"):
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Composição da Receita (Prevista, com desc.)")
                if not df_receita_prevista_mes.empty and total_receita_prevista > 0 and 'Plano' in df_receita_prevista_mes.columns:
                    df_receita_por_plano = df_receita_prevista_mes.groupby('Plano')[
                        'Valor_Plano_Final'].sum().reset_index()
                    df_receita_por_plano['Percentual'] = (
                            df_receita_por_plano['Valor_Plano_Final'] / total_receita_prevista).apply(
                        lambda x: f"{x:.1%}")
                    base_receita = alt.Chart(df_receita_por_plano).encode(
                        theta=alt.Theta("Valor_Plano_Final:Q", stack=True))
                    donut_receita = base_receita.mark_arc(outerRadius=100, innerRadius=60).encode(
                        color=alt.Color("Plano:N", title="Plano", scale=alt.Scale(scheme='spectral')),
                        order=alt.Order("Valor_Plano_Final", sort="descending"),
                        tooltip=["Plano", alt.Tooltip("Valor_Plano_Final", format=",.2f", title="Valor (R$)"),
                                 "Percentual"])
                    text_receita = base_receita.mark_text(radius=120).encode(text=alt.Text("Percentual:N"),
                                                                             order=alt.Order("Valor_Plano_Final",
                                                                                             sort="descending"),
                                                                             color=alt.value("white"))
                    st.altair_chart(donut_receita + text_receita, use_container_width=True)
                else:
                    st.info("Nenhuma receita prevista para este mês.")

            with col2:
                st.subheader(f"Composição dos Gastos (Previstos)")
                if total_gastos_previstos > 0:
                    df_gastos_composicao = df_despesas_mes.groupby('Tipo')['Valor'].sum().reset_index()
                    df_gastos_composicao = df_gastos_composicao[df_gastos_composicao['Valor'] > 0]
                    df_gastos_composicao['Percentual'] = (df_gastos_composicao['Valor'] / total_gastos_previstos).apply(
                        lambda x: f"{x:.1%}")
                    base_gastos = alt.Chart(df_gastos_composicao).encode(theta=alt.Theta("Valor:Q", stack=True))
                    donut_gastos = base_gastos.mark_arc(outerRadius=100, innerRadius=60).encode(
                        color=alt.Color("Tipo:N", title="Tipo de Gasto",
                                        scale=alt.Scale(range=['#d62728', '#ff7f0e', '#9467bd'])),
                        order=alt.Order("Valor", sort="descending"),
                        tooltip=["Tipo", alt.Tooltip("Valor", format=",.2f", title="Valor (R$)"), "Percentual"])
                    text_gastos = base_gastos.mark_text(radius=120).encode(text=alt.Text("Percentual:N"),
                                                                           order=alt.Order("Valor", sort="descending"),
                                                                           color=alt.value("white"))
                    st.altair_chart(donut_gastos + text_gastos, use_container_width=True)
                else:
                    st.info("Nenhum gasto previsto para este mês.")

            st.divider()

            # Perspectiva Anual
            st.subheader(f"Perspectiva Anual (Iniciando em {LISTA_MESES_NOMES[mes_selecionado]}/{ano_selecionado})")
            meses_futuros = [data_filtro_inicio + relativedelta(months=i) for i in range(12)]

            gastos_por_mes = {}
            if not df_despesas.empty:
                for mes_ref in meses_futuros:
                    mes_num, ano_num = mes_ref.month, mes_ref.year
                    df_lancados_mes = df_despesas[
                        (df_despesas['Mes_Competencia'] == mes_num) & (df_despesas['Ano_Competencia'] == ano_num)]
                    gastos_mes_atual = df_lancados_mes.get('Valor', 0).sum()
                    gastos_por_mes[mes_ref.strftime('%Y-%m')] = gastos_mes_atual
            df_gastos_anual = pd.DataFrame.from_dict(gastos_por_mes, orient='index',
                                                     columns=['Gastos Previstos']).reset_index().rename(
                columns={'index': 'Mes'})

            receita_por_mes = {}
            descontos_por_mes = {}
            if not df_planos.empty and not df_matriculas.empty and 'Data_Fim' in df_receita_join.columns:
                for mes_ref in meses_futuros:
                    inicio_mes_ref = datetime(mes_ref.year, mes_ref.month, 1)
                    ultimo_dia_mes_ref = calendar.monthrange(mes_ref.year, mes_ref.month)[1]
                    fim_mes_ref = datetime(mes_ref.year, mes_ref.month, ultimo_dia_mes_ref)

                    df_receita_mensal = df_receita_join[(df_receita_join['Data_Inicio'] <= fim_mes_ref) & (
                            df_receita_join['Data_Fim'] > inicio_mes_ref)]

                    if not df_receita_mensal.empty:
                        preco_col_anual = df_receita_mensal.get('Preco_Mensal', 0)
                        desc_col_anual = df_receita_mensal.get('Desconto_Percentual', 0)
                        df_receita_mensal['Valor_Final'] = preco_col_anual * (1 - desc_col_anual / 100)
                        df_receita_mensal['Valor_Descontado_Anual'] = preco_col_anual - df_receita_mensal['Valor_Final']
                        receita_por_mes[mes_ref.strftime('%Y-%m')] = df_receita_mensal['Valor_Final'].sum()
                        descontos_por_mes[mes_ref.strftime('%Y-%m')] = df_receita_mensal['Valor_Descontado_Anual'].sum()
                    else:
                        receita_por_mes[mes_ref.strftime('%Y-%m')] = 0
                        descontos_por_mes[mes_ref.strftime('%Y-%m')] = 0

            df_receita_anual = pd.DataFrame.from_dict(receita_por_mes, orient='index',
                                                      columns=['Receita Prevista']).reset_index().rename(
                columns={'index': 'Mes'})
            df_descontos_anual = pd.DataFrame.from_dict(descontos_por_mes, orient='index',
                                                        columns=['Valor_Descontado']).reset_index().rename(
                columns={'index': 'Mes'})

            st.markdown("#### Projeção de Descontos Concedidos (12 Meses)")
            chart_descontos_anual = alt.Chart(df_descontos_anual).mark_bar(color='#ff7f0e').encode(
                x=alt.X('Mes:O', title='Mês'),
                y=alt.Y('Valor_Descontado:Q', title='Total Descontado (R$)'),
                tooltip=['Mes', alt.Tooltip('Valor_Descontado', format=",.2f", title="Desconto (R$)")]
            ).properties(height=300).interactive()
            st.altair_chart(chart_descontos_anual, use_container_width=True)

            st.markdown("#### Projeção de Receita Prevista vs. Gastos Previstos (12 Meses)")
            df_comparativo_anual = pd.merge(df_gastos_anual, df_receita_anual, on='Mes', how='outer').fillna(0)
            df_comparativo_anual_melted = df_comparativo_anual.melt('Mes', var_name='Tipo', value_name='Valor')
            chart_anual = alt.Chart(df_comparativo_anual_melted).mark_bar().encode(
                x=alt.X('Mes:O', title='Perspectiva 12 Meses'),
                y=alt.Y('Valor:Q', title='Valor (R$)'),
                color=alt.Color('Tipo:N', scale=alt.Scale(domain=['Receita Prevista', 'Gastos Previstos'],
                                                          range=['#2ca02c', '#d62728']),
                                legend=alt.Legend(title="Tipo de Fluxo")),
                xOffset='Tipo:N',
                tooltip=['Mes', 'Tipo', alt.Tooltip('Valor', format=",.2f")]
            ).properties(height=400).interactive()
            st.altair_chart(chart_anual, use_container_width=True)

            with st.expander("Ver Tabelas de Detalhes de Gastos (Previstos) do Mês"):
                st.subheader(f"Contas a Pagar Lançadas em {LISTA_MESES_NOMES[mes_selecionado]}")
                cols_gastos = ['ID', 'Descricao', 'Valor', 'Status_Pagamento', 'Valor_Pago', 'Data_Vencimento',
                               'Recorrente', 'Tipo']
                cols_gastos_existem = [col for col in cols_gastos if col in df_despesas_mes.columns]
                st.dataframe(df_despesas_mes[cols_gastos_existem], use_container_width=True)

    except gspread.exceptions.WorksheetNotFound:
        st.error(
            "Erro Crítico: Abas essenciais não encontradas. Verifique `Matriculas`, `Planos`, `Lancamentos_Despesas`, etc.")
    except Exception as e:
        st.exception(f"Ocorreu um erro inesperado ao gerar o dashboard: {e}")
        st.error("Possível causa: Colunas essenciais faltando ou renomeadas nas planilhas.")


# -----------------------------------------------------
# PÁGINA: GESTÃO DE RENOVAÇÕES
# -----------------------------------------------------
def pagina_renovacoes():
    st.title("🔔 Gestão de Renovações")
    st.write("Controle aqui os alunos com planos vencidos ou prestes a vencer.")

    try:
        df_matriculas = load_matriculas()
        df_planos = load_planos()

        if df_matriculas.empty or 'Status' not in df_matriculas.columns:
            st.error("Não foi possível carregar as matrículas ou a coluna 'Status' está faltando.")
            st.stop()
        if df_planos.empty or 'Duracao_Meses' not in df_planos.columns:
            st.error("Não foi possível carregar os planos ou a coluna 'Duracao_Meses' está faltando.")
            st.stop()

        df_ativas = df_matriculas[df_matriculas['Status'].str.lower() == 'ativa'].copy()
        if df_ativas.empty:
            st.info("Nenhum aluno(a) 'Ativo(a)' encontrado para verificar renovações.")
            st.stop()

        df_merged = df_ativas.merge(df_planos, on='Plano', how='left')
        df_merged['Duracao_Meses'] = df_merged['Duracao_Meses'].fillna(0)
        df_merged = df_merged.dropna(subset=['Data_Inicio'])
        df_merged['Data_Fim'] = df_merged.apply(
            lambda row: row['Data_Inicio'] + pd.DateOffset(months=int(row['Duracao_Meses']))
            if pd.notna(row['Data_Inicio']) and row['Duracao_Meses'] > 0 else pd.NaT,
            axis=1
        )
        df_merged = df_merged.dropna(subset=['Data_Fim'])

        hoje_dt = hoje
        limite_30_dias = hoje_dt + relativedelta(days=30)

        df_expirados = df_merged[df_merged['Data_Fim'].dt.date < hoje_dt.date()].sort_values(by='Data_Fim',
                                                                                             ascending=True)
        df_a_vencer = df_merged[
            (df_merged['Data_Fim'].dt.date >= hoje_dt.date()) & (df_merged['Data_Fim'].dt.date <= limite_30_dias.date())
            ].sort_values(by='Data_Fim', ascending=True)

        # Seção 1: Planos Expirados
        st.subheader("⚠️ Planos Expirados (Ação Imediata)")
        if df_expirados.empty:
            st.info("Nenhum aluno(a) ativo(a) com plano expirado.")
        else:
            st.warning(f"Encontrados {len(df_expirados)} alunos(as) com status 'Ativa' mas plano expirado.")
            lista_planos_nomes = df_planos['Plano'].tolist()

            for _, aluno in df_expirados.iterrows():
                id_aluno = aluno['ID']
                data_fim_str = aluno['Data_Fim'].strftime('%d/%m/%Y')
                with st.expander(f"**{aluno['Nome']}** - (Plano: {aluno['Plano']}) - Vence em **{data_fim_str}**"):
                    col1, col2 = st.columns([0.6, 0.4])
                    with col1:
                        st.markdown("#### Ação 1: Renovar Plano")
                        with st.form(f"form_renovar_{id_aluno}", clear_on_submit=True):
                            try:
                                plano_atual_idx = lista_planos_nomes.index(aluno['Plano'])
                            except ValueError:
                                plano_atual_idx = 0
                            novo_plano = st.selectbox("Selecione o Plano", lista_planos_nomes, index=plano_atual_idx,
                                                      key=f"plano_{id_aluno}")
                            nova_data_inicio = st.date_input("Nova Data de Início", value=aluno['Data_Fim'].date(),
                                                             key=f"data_{id_aluno}")

                            if novo_plano:
                                plano_info = df_planos[df_planos['Plano'] == novo_plano].iloc[0]
                                st.info(
                                    f"Projeção: R$ {plano_info['Preco_Mensal']:,.2f}/mês por {int(plano_info['Duracao_Meses'])} meses.")

                            novo_desconto = st.number_input("Desconto (%)",
                                                            value=float(aluno.get('Desconto_Percentual', 0.0)),
                                                            min_value=0.0, max_value=25.0, step=1.0,
                                                            key=f"desc_{id_aluno}")
                            nova_justificativa = st.text_input("Justificativa do Desconto",
                                                               value=aluno.get('Justificativa_Desconto', ''),
                                                               key=f"just_{id_aluno}")
                            submitted_renew = st.form_submit_button("✅ Confirmar Renovação")

                        if submitted_renew:
                            if novo_desconto > 0 and not nova_justificativa:
                                st.warning("Por favor, insira a 'Justificativa do Desconto'.")
                                st.stop()
                            else:
                                dados_renovacao = {
                                    "Plano": novo_plano, "Data_Inicio": nova_data_inicio.strftime("%Y-%m-%d"),
                                    "Status": "Ativa", "Desconto_Percentual": novo_desconto,
                                    "Justificativa_Desconto": nova_justificativa,
                                    "Data_Congelamento_Inicio": ""
                                }

                                try:
                                    plano_info_hist = df_planos[df_planos['Plano'] == novo_plano].iloc[0]
                                    preco_plano_hist = plano_info_hist['Preco_Mensal']
                                    valor_final_contrato = preco_plano_hist * (1 - novo_desconto / 100)
                                    df_hist = load_historico_renovacoes()
                                    id_hist = (df_hist['ID_Historico'].max() + 1) if not df_hist.empty and not df_hist[
                                        'ID_Historico'].isnull().all() else 1
                                    linha_historico = [
                                        id_hist, id_aluno, aluno['Nome'], novo_plano,
                                        nova_data_inicio.strftime("%Y-%m-%d"), valor_final_contrato,
                                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    ]
                                    hist_ws = sheet.worksheet("Historico_Renovacoes")
                                    hist_ws.append_row([str(item) for item in linha_historico],
                                                       value_input_option='USER_ENTERED')
                                except Exception as e_hist:
                                    st.error(f"Erro ao salvar no histórico de renovações: {e_hist}")

                                if atualizar_matricula_aluno(id_aluno, dados_renovacao):
                                    st.success(f"{aluno['Nome']} renovado(a) com sucesso!")
                                    clear_all_caches();
                                    time.sleep(1);
                                    st.rerun()
                                else:
                                    st.error("Falha ao renovar.")
                    with col2:
                        st.markdown("#### Ação 2: Não Renovar")
                        with st.form(f"form_inativar_{id_aluno}", clear_on_submit=True):
                            st.write(f"Mudar status de {aluno['Nome']} para:")
                            novo_status = st.selectbox("Novo Status", ["Inativa", "Cancelada"], index=0,
                                                       label_visibility="collapsed", key=f"status_{id_aluno}")
                            submitted_inativar = st.form_submit_button("❌ Confirmar Inativação")
                        if submitted_inativar:
                            if atualizar_matricula_aluno(id_aluno,
                                                         {"Status": novo_status, "Data_Congelamento_Inicio": ""}):
                                st.success(f"Status de {aluno['Nome']} alterado para '{novo_status}'.")
                                clear_all_caches();
                                time.sleep(1);
                                st.rerun()
                            else:
                                st.error("Falha ao inativar.")

        st.divider()

        # Seção 2: Planos a Vencer
        st.subheader("🔔 Planos Vencendo nos Próximos 30 Dias")
        if df_a_vencer.empty:
            st.info("Nenhum aluno(a) com plano vencendo nos próximos 30 dias.")
        else:
            st.info(f"Encontrados {len(df_a_vencer)} alunos(as) com planos prestes a vencer.")
            lista_planos_nomes = df_planos['Plano'].tolist()

            for _, aluno in df_a_vencer.iterrows():
                id_aluno = aluno['ID']
                data_fim_str = aluno['Data_Fim'].strftime('%d/%m/%Y')
                with st.expander(f"**{aluno['Nome']}** - (Plano: {aluno['Plano']}) - Vence em **{data_fim_str}**"):
                    col1, col2 = st.columns([0.6, 0.4])
                    with col1:
                        st.markdown("#### Ação 1: Renovar Plano")
                        with st.form(f"form_renovar_{id_aluno}", clear_on_submit=True):
                            try:
                                plano_atual_idx = lista_planos_nomes.index(aluno['Plano'])
                            except ValueError:
                                plano_atual_idx = 0
                            novo_plano = st.selectbox("Selecione o Plano", lista_planos_nomes, index=plano_atual_idx,
                                                      key=f"plano_{id_aluno}")
                            nova_data_inicio = st.date_input("Nova Data de Início", value=aluno['Data_Fim'].date(),
                                                             key=f"data_{id_aluno}")

                            if novo_plano:
                                plano_info = df_planos[df_planos['Plano'] == novo_plano].iloc[0]
                                st.info(
                                    f"Projeção: R$ {plano_info['Preco_Mensal']:,.2f}/mês por {int(plano_info['Duracao_Meses'])} meses.")

                            novo_desconto = st.number_input("Desconto (%)",
                                                            value=float(aluno.get('Desconto_Percentual', 0.0)),
                                                            min_value=0.0, max_value=25.0, step=1.0,
                                                            key=f"desc_{id_aluno}")
                            nova_justificativa = st.text_input("Justificativa do Desconto",
                                                               value=aluno.get('Justificativa_Desconto', ''),
                                                               key=f"just_{id_aluno}")
                            submitted_renew = st.form_submit_button("✅ Confirmar Renovação")

                        if submitted_renew:
                            if novo_desconto > 0 and not nova_justificativa:
                                st.warning("Por favor, insira a 'Justificativa do Desconto'.")
                                st.stop()
                            else:
                                dados_renovacao = {
                                    "Plano": novo_plano, "Data_Inicio": nova_data_inicio.strftime("%Y-%m-%d"),
                                    "Status": "Ativa", "Desconto_Percentual": novo_desconto,
                                    "Justificativa_Desconto": nova_justificativa,
                                    "Data_Congelamento_Inicio": ""
                                }

                                try:
                                    plano_info_hist = df_planos[df_planos['Plano'] == novo_plano].iloc[0]
                                    preco_plano_hist = plano_info_hist['Preco_Mensal']
                                    valor_final_contrato = preco_plano_hist * (1 - novo_desconto / 100)
                                    df_hist = load_historico_renovacoes()
                                    id_hist = (df_hist['ID_Historico'].max() + 1) if not df_hist.empty and not df_hist[
                                        'ID_Historico'].isnull().all() else 1
                                    linha_historico = [
                                        id_hist, id_aluno, aluno['Nome'], novo_plano,
                                        nova_data_inicio.strftime("%Y-%m-%d"), valor_final_contrato,
                                        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                    ]
                                    hist_ws = sheet.worksheet("Historico_Renovacoes")
                                    hist_ws.append_row([str(item) for item in linha_historico],
                                                       value_input_option='USER_ENTERED')
                                except Exception as e_hist:
                                    st.error(f"Erro ao salvar no histórico de renovações: {e_hist}")

                                if atualizar_matricula_aluno(id_aluno, dados_renovacao):
                                    st.success(f"{aluno['Nome']} renovado(a) com sucesso!")
                                    clear_all_caches();
                                    time.sleep(1);
                                    st.rerun()
                                else:
                                    st.error("Falha ao renovar.")
                    with col2:
                        st.markdown("#### Ação 2: Não Renovar")
                        with st.form(f"form_inativar_{id_aluno}", clear_on_submit=True):
                            st.write(f"Mudar status de {aluno['Nome']} para:")
                            novo_status = st.selectbox("Novo Status", ["Inativa", "Cancelada"], index=0,
                                                       label_visibility="collapsed", key=f"status_{id_aluno}")
                            submitted_inativar = st.form_submit_button("❌ Confirmar Inativação")

                        if submitted_inativar:
                            if atualizar_matricula_aluno(id_aluno,
                                                         {"Status": novo_status, "Data_Congelamento_Inicio": ""}):
                                st.success(f"Status de {aluno['Nome']} alterado para '{novo_status}'.")
                                clear_all_caches();
                                time.sleep(1);
                                st.rerun()
                            else:
                                st.error("Falha ao inativar.")
    except Exception as e:
        st.exception(f"Ocorreu um erro inesperado ao gerar a página de renovações: {e}")


# -----------------------------------------------------
# PÁGINA: GERENCIAR STATUS (CONGELAR/REATIVAR)
# -----------------------------------------------------
def pagina_gerenciar_status():
    st.title("🧊 Gerenciar Status (Congelar/Reativar)")
    st.write("Use esta página para pausar (congelar) ou retomar (reativar) os contratos dos alunos.")

    try:
        df_matriculas = load_matriculas()
        df_planos = load_planos()

        if df_matriculas.empty or df_planos.empty:
            st.error("Dados de matrículas ou planos não carregados.")
            st.stop()

    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        st.stop()

    st.subheader("1. Congelar Matrícula (Pausar Contrato)")
    df_ativas = df_matriculas[df_matriculas['Status'].str.lower() == 'ativa']

    if df_ativas.empty:
        st.info("Nenhum aluno 'Ativo' para congelar.")
    else:
        dict_ativas = pd.Series(df_ativas.ID.values, index=df_ativas.Nome).to_dict()
        nome_aluno_cong = st.selectbox("Selecione um aluno(a) ATIVO para congelar:",
                                       options=dict_ativas.keys(), index=None, placeholder="Selecione...")

        if nome_aluno_cong:
            id_aluno_sel = dict_ativas[nome_aluno_cong]
            aluno = df_ativas[df_ativas['ID'] == id_aluno_sel].iloc[0]

            with st.form(f"form_congelar_{id_aluno_sel}", clear_on_submit=True):
                st.warning(f"Você está prestes a congelar a matrícula de **{aluno['Nome']}**.")
                data_congelamento = st.date_input("Data do Início do Congelamento*", value=datetime.now())

                submitted_cong = st.form_submit_button("Confirmar Congelamento")

            if submitted_cong:
                dados_update = {
                    "Status": "Congelado",
                    "Data_Congelamento_Inicio": data_congelamento.strftime("%Y-%m-%d")
                }
                if atualizar_matricula_aluno(id_aluno_sel, dados_update):
                    st.success(
                        f"Matrícula de {aluno['Nome']} congelada com sucesso a partir de {data_congelamento.strftime('%d/%m/%Y')}.")
                    clear_all_caches()
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error("Erro ao tentar congelar a matrícula.")

    st.divider()

    st.subheader("2. Reativar Matrícula (Retomar Contrato)")
    df_congelados = df_matriculas[df_matriculas['Status'].str.lower() == 'congelado']

    if df_congelados.empty:
        st.info("Nenhum aluno 'Congelado' para reativar.")
    else:
        dict_congelados = pd.Series(df_congelados.ID.values, index=df_congelados.Nome).to_dict()
        nome_aluno_reat = st.selectbox("Selecione um aluno(a) CONGELADO para reativar:",
                                       options=dict_congelados.keys(), index=None, placeholder="Selecione...")

        if nome_aluno_reat:
            id_aluno_sel = dict_congelados[nome_aluno_reat]
            aluno = df_congelados[df_congelados['ID'] == id_aluno_sel].iloc[0]

            data_cong_inicio = aluno.get('Data_Congelamento_Inicio')
            data_inicio_original = aluno.get('Data_Inicio')

            if pd.isna(data_cong_inicio) or pd.isna(data_inicio_original):
                st.error(
                    f"Erro: {aluno['Nome']} está 'Congelado' mas não possui 'Data_Inicio' ou 'Data_Congelamento_Inicio' na planilha. Não é possível reativar.")
                st.stop()

            data_cong_inicio_dt = datetime.combine(data_cong_inicio, dt_time.min)

            with st.form(f"form_reativar_{id_aluno_sel}", clear_on_submit=True):
                st.info(f"Aluno(a) congelado(a) desde: {data_cong_inicio.strftime('%d/%m/%Y')}")
                data_reativacao = st.date_input("Data de Reativação*", value=datetime.now())

                dias_congelados = (data_reativacao - data_cong_inicio).days

                disable_button = False
                nova_data_inicio = data_inicio_original

                if dias_congelados < 1:
                    st.warning("A data de reativação deve ser pelo menos um dia após o início do congelamento.")
                    disable_button = True
                else:
                    nova_data_inicio = data_inicio_original + relativedelta(days=dias_congelados)
                    st.success(f"O contrato será estendido em {dias_congelados} dias.")
                    st.markdown(
                        f"A Data de Início do contrato será movida de `{data_inicio_original.strftime('%d/%m/%Y')}` para `{nova_data_inicio.strftime('%d/%m/%Y')}`.")

                submitted_reat = st.form_submit_button("Confirmar Reativação", disabled=disable_button)

            if submitted_reat:
                dados_update = {
                    "Status": "Ativa",
                    "Data_Inicio": nova_data_inicio.strftime("%Y-%m-%d"),
                    "Data_Congelamento_Inicio": ""
                }
                if atualizar_matricula_aluno(id_aluno_sel, dados_update):
                    st.success(
                        f"Matrícula de {aluno['Nome']} reativada com sucesso! O contrato foi estendido em {dias_congelados} dias.")
                    clear_all_caches()
                    time.sleep(2)
                    st.rerun()
                else:
                    st.error("Erro ao tentar reativar a matrícula.")


# -----------------------------------------------------
# PÁGINA: RESERVA (INVESTIMENTOS)
# -----------------------------------------------------
def pagina_investimentos():
    st.title("🏦 Reserva de Oportunidade (Investimentos)")
    st.info(
        "Aqui você gerencia sua reserva de caixa (meta: 12x o faturamento mensal). Os cálculos de rendimento são projeções brutas (sem IR).")

    try:
        df_movimentacoes = load_investimentos()
    except gspread.exceptions.WorksheetNotFound:
        st.error("Aba 'Investimentos_Caixa' não encontrada na planilha!")
        st.error("Por favor, crie a aba com os cabeçalhos: ID_Movimentacao, Data, Tipo, Produto, Valor, Descricao")
        st.stop()
    except Exception as e:
        st.error(f"Erro ao carregar investimentos: {e}")
        st.stop()

    st.subheader("Parâmetros de Projeção")
    taxa_cdi_input = st.number_input(
        "Taxa CDI/Selic Anual Atual (%)",
        min_value=0.1,
        value=10.50,
        step=0.1,
        format="%.2f",
        help="Insira a taxa Selic ou CDI atual (Ex: 10.50 para 10,50% a.a.)"
    )
    taxa_cdi_anual = taxa_cdi_input / 100.0

    # Cálculo de Saldos
    if df_movimentacoes.empty:
        saldo_cdb100 = 0.0
        saldo_cdb102 = 0.0
    else:
        df_saldo = df_movimentacoes.groupby('Produto')['Valor'].sum().reset_index()
        saldo_cdb100 = df_saldo[df_saldo['Produto'] == 'CDB 100% CDI']['Valor'].sum()
        saldo_cdb102 = df_saldo[df_saldo['Produto'] == 'CDB 102% CDI']['Valor'].sum()
    saldo_total = saldo_cdb100 + saldo_cdb102

    st.divider()
    st.metric("Saldo Total Investido (Principal)", f"R$ {saldo_total:,.2f}",
              help="Soma de todos os aportes menos todos os resgates. Não inclui rendimentos.")

    # Cálculo de Rendimentos
    st.subheader("Projeção de Rendimento Bruto (Diário e Mensal)")
    taxa_diaria_bruta = (1 + taxa_cdi_anual) ** (1 / 252) - 1
    taxa_mensal_bruta = (1 + taxa_cdi_anual) ** (1 / 12) - 1

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("##### CDB 100% CDI (Liquidez D+0)")
        st.metric("Saldo (Principal)", f"R$ {saldo_cdb100:,.2f}")
        rend_dia_100 = saldo_cdb100 * (taxa_diaria_bruta * 1.00)
        rend_mes_100 = saldo_cdb100 * (taxa_mensal_bruta * 1.00)
        st.metric("Rendimento Diário (Bruto)", f"R$ {rend_dia_100:,.2f}")
        st.metric("Rendimento Mensal (Bruto)", f"R$ {rend_mes_100:,.2f}")
    with col2:
        st.markdown("##### CDB 102% CDI (Liquidez D+2)")
        st.metric("Saldo (Principal)", f"R$ {saldo_cdb102:,.2f}")
        rend_dia_102 = saldo_cdb102 * (taxa_diaria_bruta * 1.02)
        rend_mes_102 = saldo_cdb102 * (taxa_mensal_bruta * 1.02)
        st.metric("Rendimento Diário (Bruto)", f"R$ {rend_dia_102:,.2f}")
        st.metric("Rendimento Mensal (Bruto)", f"R$ {rend_mes_102:,.2f}")

    st.divider()

    # Formulários de Aporte e Resgate
    st.subheader("Movimentar Valores")
    lista_produtos = ["CDB 100% CDI", "CDB 102% CDI"]

    col3, col4 = st.columns(2)
    with col3:
        with st.form("form_aporte", clear_on_submit=True):
            st.markdown("#### Fazer Aporte")
            data_aporte = st.date_input("Data do Aporte", value=datetime.now())
            produto_aporte = st.selectbox("Alocar em qual produto?", lista_produtos, index=0)
            valor_aporte = st.number_input("Valor do Aporte (R$)", min_value=0.01, format="%.2f")
            desc_aporte = st.text_input("Descrição (Opcional)", placeholder="Ex: Aporte mensal")
            submitted_aporte = st.form_submit_button("Confirmar Aporte")

    with col4:
        with st.form("form_resgate", clear_on_submit=True):
            st.markdown("#### Fazer Resgate")
            data_resgate = st.date_input("Data do Resgate", value=datetime.now())
            produto_resgate = st.selectbox("Resgatar de qual produto?", lista_produtos, index=0)
            valor_resgate = st.number_input("Valor do Resgate (R$)", min_value=0.01, format="%.2f")
            desc_resgate = st.text_input("Descrição (Opcional)", placeholder="Ex: Resgate para pagar 13º")
            submitted_resgate = st.form_submit_button("Confirmar Resgate")

    if submitted_aporte:
        if not valor_aporte or valor_aporte <= 0:
            st.warning("Insira um valor de aporte válido.")
            st.stop()
        try:
            df_mov = load_investimentos()
            novo_id = (df_mov['ID_Movimentacao'].max() + 1) if not df_mov.empty and not df_mov[
                'ID_Movimentacao'].isnull().all() else 1
            nova_linha = [novo_id, data_aporte.strftime("%Y-%m-%d"), "Aporte", produto_aporte, valor_aporte,
                          desc_aporte]

            invest_ws = sheet.worksheet("Investimentos_Caixa")
            invest_ws.append_row([str(item) for item in nova_linha], value_input_option='USER_ENTERED')
            clear_all_caches()
            st.success(f"Aporte de R$ {valor_aporte:,.2f} registrado com sucesso!")
            time.sleep(1);
            st.rerun()
        except Exception as e:
            st.error(f"Erro ao salvar aporte: {e}")

    if submitted_resgate:
        if not valor_resgate or valor_resgate <= 0:
            st.warning("Insira um valor de resgate válido.")
            st.stop()

        saldo_atual_produto = saldo_cdb100 if produto_resgate == "CDB 100% CDI" else saldo_cdb102
        if valor_resgate > saldo_atual_produto:
            st.error(
                f"Saldo insuficiente! Você tentou resgatar R$ {valor_resgate:,.2f}, mas o saldo (principal) deste produto é de R$ {saldo_atual_produto:,.2f}.")
            st.stop()
        else:
            try:
                df_mov = load_investimentos()
                novo_id = (df_mov['ID_Movimentacao'].max() + 1) if not df_mov.empty and not df_mov[
                    'ID_Movimentacao'].isnull().all() else 1
                nova_linha = [novo_id, data_resgate.strftime("%Y-%m-%d"), "Resgate", produto_resgate, -valor_resgate,
                              desc_resgate]

                invest_ws = sheet.worksheet("Investimentos_Caixa")
                invest_ws.append_row([str(item) for item in nova_linha], value_input_option='USER_ENTERED')
                clear_all_caches()
                st.success(f"Resgate de R$ {valor_resgate:,.2f} registrado com sucesso!")
                time.sleep(1);
                st.rerun()
            except Exception as e:
                st.error(f"Erro ao salvar resgate: {e}")

    st.divider()

    # Gráfico de Projeção
    st.subheader("Projeção de Juros Compostos (Bruto)")
    meses_projecao = st.slider("Projetar Saldo para (meses):", min_value=1, max_value=60, value=12)

    data_projecao = []
    taxa_mensal_100 = taxa_mensal_bruta * 1.00
    taxa_mensal_102 = taxa_mensal_bruta * 1.02

    s_100_proj = saldo_cdb100
    s_102_proj = saldo_cdb102

    for m in range(meses_projecao + 1):
        if m == 0:
            s_100, s_102 = saldo_cdb100, saldo_cdb102
        else:
            s_100 = s_100_proj * (1 + taxa_mensal_100) ** m
            s_102 = s_102_proj * (1 + taxa_mensal_102) ** m

        data_projecao.append({'Mês': m, 'Produto': 'CDB 100% CDI', 'Saldo Projetado': s_100})
        data_projecao.append({'Mês': m, 'Produto': 'CDB 102% CDI', 'Saldo Projetado': s_102})
        data_projecao.append({'Mês': m, 'Produto': 'Total', 'Saldo Projetado': s_100 + s_102})

    df_proj = pd.DataFrame(data_projecao)
    chart = alt.Chart(df_proj).mark_line(point=True).encode(
        x=alt.X('Mês:O', axis=alt.Axis(title='Meses no Futuro')),
        y=alt.Y('Saldo Projetado:Q', axis=alt.Axis(title='Saldo (R$)', format=",.2f")),
        color=alt.Color('Produto:N', title="Produto"),
        tooltip=['Mês', 'Produto', alt.Tooltip('Saldo Projetado', format=",.2f")]
    ).properties(height=400).interactive()
    st.altair_chart(chart, use_container_width=True)

    # Histórico de Movimentações
    st.subheader("Histórico de Movimentações")
    with st.expander("Ver todas as movimentações"):
        if df_movimentacoes.empty:
            st.info("Nenhuma movimentação (aporte ou resgate) registrada.")
        else:
            st.dataframe(df_movimentacoes.sort_values(by="Data", ascending=False), use_container_width=True)


# -----------------------------------------------------
# PÁGINA: RELATÓRIO DE RENOVAÇÕES (HISTÓRICO)
# -----------------------------------------------------
def pagina_relatorio_renovacoes():
    st.title("📊 Relatório de Renovações (Histórico)")
    st.write("Visualize o histórico de todos os contratos (novos e renovações) por ano.")

    try:
        df_historico = load_historico_renovacoes()
    except gspread.exceptions.WorksheetNotFound:
        st.error("Aba 'Historico_Renovacoes' não encontrada!")
        st.info(
            "Para ativar este relatório, crie a aba 'Historico_Renovacoes' na sua planilha com as colunas: ID_Historico, ID_Aluno, Nome_Aluno, Plano, Data_Inicio_Contrato, Valor_Contrato, Data_Registro")
        st.stop()
    except Exception as e:
        st.error(f"Erro ao carregar histórico: {e}")
        st.stop()

    if df_historico.empty:
        st.info("Nenhum histórico de contrato encontrado. Cadastre ou renove alunos para popular esta página.")
        st.stop()

    df_historico['Ano_Contrato'] = df_historico['Data_Inicio_Contrato'].dt.year

    anos_disponiveis = sorted(df_historico['Ano_Contrato'].unique(), reverse=True)
    ano_selecionado = st.selectbox("Selecione o Ano para analisar:", anos_disponiveis, index=0)

    df_filtrado = df_historico[df_historico['Ano_Contrato'] == ano_selecionado]

    if df_filtrado.empty:
        st.warning(f"Nenhum contrato iniciado no ano {ano_selecionado}.")
        st.stop()

    total_contratos_ano = len(df_filtrado)
    valor_total_contratos_ano = df_filtrado['Valor_Contrato'].sum()

    col1, col2 = st.columns(2)
    col1.metric(f"Total de Contratos ({ano_selecionado})", f"{total_contratos_ano} (Novos + Renovações)")
    col2.metric(f"Valor Mensal Contratado ({ano_selecionado})", f"R$ {valor_total_contratos_ano:,.2f} /mês",
                help="Soma do valor mensal de todos os contratos iniciados neste ano.")

    st.divider()

    st.subheader(f"Detalhes dos Contratos de {ano_selecionado}")

    cols_display = ['Data_Inicio_Contrato', 'Nome_Aluno', 'Plano', 'Valor_Contrato', 'ID_Aluno']
    df_display_hist = df_filtrado[cols_display].sort_values(by="Data_Inicio_Contrato", ascending=False)

    df_display_hist.rename(columns={
        'Data_Inicio_Contrato': 'Data de Início',
        'Nome_Aluno': 'Aluno(a)',
        'Valor_Contrato': 'Valor Mensal (R$)'
    }, inplace=True)

    if 'Valor Mensal (R$)' in df_display_hist:
        df_display_hist['Valor Mensal (R$)'] = df_display_hist['Valor Mensal (R$)'].map("R$ {:,.2f}".format)
    if 'Data de Início' in df_display_hist:
        df_display_hist['Data de Início'] = df_display_hist['Data de Início'].dt.strftime('%d/%m/%Y')

    st.dataframe(df_display_hist, use_container_width=True)


# -----------------------------------------------------
# PÁGINA: ANIVERSARIANTES
# -----------------------------------------------------
def pagina_aniversariantes():
    st.title("🎂 Aniversariantes e Celebrações")
    st.write("Veja os aniversários de vida e de studio para este mês e o próximo.")

    try:
        df_matriculas = load_matriculas()
        if df_matriculas.empty:
            st.error("Não foi possível carregar as matrículas.")
            st.stop()

        df_ativas = df_matriculas[df_matriculas['Status'].str.lower() == 'ativa'].copy()
        if df_ativas.empty:
            st.info("Nenhum aluno(a) 'Ativo(a)' encontrado.")
            st.stop()

    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        st.stop()

    mes_seguinte = (hoje + relativedelta(months=1)).month
    nome_mes_atual = LISTA_MESES_NOMES[MES_ATUAL]
    nome_mes_seguinte = LISTA_MESES_NOMES[mes_seguinte]

    st.subheader(f"Filtrando para: {nome_mes_atual} e {nome_mes_seguinte}")

    if 'Data_Nascimento' in df_ativas.columns:
        df_ativas['Mes_Nasc'] = df_ativas['Data_Nascimento'].dt.month
        df_ativas['Dia_Nasc'] = df_ativas['Data_Nascimento'].dt.day
    else:
        st.error("Coluna 'Data_Nascimento' não encontrada em Matrículas.")
        df_ativas['Mes_Nasc'] = pd.NaT
        df_ativas['Dia_Nasc'] = pd.NaT

    if 'Data_Primeira_Matricula' in df_ativas.columns:
        df_ativas['Mes_Studio'] = df_ativas['Data_Primeira_Matricula'].dt.month
        df_ativas['Dia_Studio'] = df_ativas['Data_Primeira_Matricula'].dt.day
        df_ativas['Anos_Studio'] = df_ativas['Data_Primeira_Matricula'].apply(
            lambda x: relativedelta(hoje.date(), x.date()).years if pd.notna(x) else 0
        )
    else:
        st.error("Coluna 'Data_Primeira_Matricula' não encontrada em Matrículas.")
        df_ativas['Mes_Studio'] = pd.NaT
        df_ativas['Dia_Studio'] = pd.NaT
        df_ativas['Anos_Studio'] = 0

    col1, col2 = st.columns(2)

    with col1:
        st.header(f"🎉 Aniversários (Vida)")
        st.markdown(f"#### {nome_mes_atual} (Mês Atual)")

        df_nasc_atual = df_ativas[df_ativas['Mes_Nasc'] == MES_ATUAL].sort_values(by="Dia_Nasc")
        if df_nasc_atual.empty:
            st.info(f"Nenhum aniversariante em {nome_mes_atual}.")
        else:
            for _, aluno in df_nasc_atual.iterrows():
                st.markdown(f"**{aluno['Dia_Nasc']:02.0f}/{aluno['Mes_Nasc']:02.0f}** - {aluno['Nome']}")

        st.markdown(f"#### {nome_mes_seguinte} (Próximo Mês)")
        df_nasc_seguinte = df_ativas[df_ativas['Mes_Nasc'] == mes_seguinte].sort_values(by="Dia_Nasc")
        if df_nasc_seguinte.empty:
            st.info(f"Nenhum aniversariante em {nome_mes_seguinte}.")
        else:
            for _, aluno in df_nasc_seguinte.iterrows():
                st.markdown(f"**{aluno['Dia_Nasc']:02.0f}/{aluno['Mes_Nasc']:02.0f}** - {aluno['Nome']}")

    with col2:
        st.header(f"⭐ Aniversários (Studio)")
        st.markdown(f"#### {nome_mes_atual} (Mês Atual)")

        df_studio_atual = df_ativas[df_ativas['Mes_Studio'] == MES_ATUAL].sort_values(by="Dia_Studio")
        if df_studio_atual.empty:
            st.info(f"Nenhum aniversário de studio em {nome_mes_atual}.")
        else:
            for _, aluno in df_studio_atual.iterrows():
                anos = aluno['Anos_Studio']
                # Ajuste para 0 anos -> 1º ano
                texto_anos = f"({anos + 1}º ano)" if anos == 0 else f"({anos} anos)"
                if anos > 0:
                    texto_anos = f"({anos} ano{'s' if anos > 1 else ''})"
                else:
                    # Se for menos de 1 ano, mas no mês de aniversário, celebramos o 1º ano.
                    texto_anos = "(1º ano)"

                st.markdown(
                    f"**{aluno['Dia_Studio']:02.0f}/{aluno['Mes_Studio']:02.0f}** - {aluno['Nome']} {texto_anos}")

        st.markdown(f"#### {nome_mes_seguinte} (Próximo Mês)")
        df_studio_seguinte = df_ativas[df_ativas['Mes_Studio'] == mes_seguinte].sort_values(by="Dia_Studio")
        if df_studio_seguinte.empty:
            st.info(f"Nenhum aniversário de studio em {nome_mes_seguinte}.")
        else:
            for _, aluno in df_studio_seguinte.iterrows():
                anos = aluno['Anos_Studio']
                # Ajuste para 0 anos -> 1º ano
                texto_anos = f"({anos + 1}º ano)" if anos == 0 else f"({anos} anos)"
                if anos > 0:
                    texto_anos = f"({anos} ano{'s' if anos > 1 else ''})"
                else:
                    texto_anos = "(1º ano)"

                st.markdown(
                    f"**{aluno['Dia_Studio']:02.0f}/{aluno['Mes_Studio']:02.0f}** - {aluno['Nome']} {texto_anos}")


# -----------------------------------------------------
# APP PRINCIPAL (Sidebar e Navegação)
# -----------------------------------------------------

if sheet:
    st.sidebar.image("logo.png", width=60)
    st.sidebar.title("Inspire Expire App")

    paginas = {
        "📈 Dashboard Financeiro": pagina_financeiro,
        "🏦 Reserva (Investimentos)": pagina_investimentos,
        "--- (Receitas) ---": None,
        "👤 Cadastrar Aluno(a)": pagina_cadastro,
        "💰 Lançar Pagamento": pagina_lancar_pagamento,
        "🔔 Gestão de Renovações": pagina_renovacoes,
        "🧊 Gerenciar Status (Congelar)": pagina_gerenciar_status,
        "📊 Relatório de Renovações": pagina_relatorio_renovacoes,
        "--- (Despesas) ---": None,
        "💸 Lançar Despesa": pagina_lancar_despesa,
        "🧾 Pagar Contas (Baixa)": pagina_contas_a_pagar,
        "--- (Consultas) ---": None,
        "🔍 Alunos e Histórico": pagina_todos_alunos,
        "✅ Registrar Presença": pagina_presenca,
        "🎂 Aniversariantes do Mês": pagina_aniversariantes,
    }

    query_params = st.query_params.to_dict()
    default_page = query_params.get("page", [list(paginas.keys())[0]])[0]

    try:
        default_index = list(paginas.keys()).index(default_page)
    except ValueError:
        default_index = 0

    escolha = st.sidebar.radio("Navegação", paginas.keys(), index=default_index, label_visibility="collapsed")

    st.sidebar.divider()
    if st.sidebar.button("🔄 Forçar Atualização dos Dados"):
        clear_all_caches()
        st.toast("Dados atualizados com sucesso!", icon="✅")

    if "page" in st.query_params:
        st.query_params.clear()

    # Lógica para pular divisores
    if paginas[escolha] is not None:
        paginas[escolha]()
    else:
        pass  # Não faz nada se clicar em um divisor

else:
    st.error("🚨 Falha na conexão com o Google Sheets. Verifique o 'secrets.toml' e as permissões de compartilhamento.")