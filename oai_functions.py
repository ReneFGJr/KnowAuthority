import requests
import xml.etree.ElementTree as ET
import mysql.connector
import database


def coletar_identify_oai(url, mysql_config):
    """
    Coleta informações do Identify do OAI-PMH e armazena no MySQL.

    :param url: URL do endpoint OAI-PMH com verb=Identify
    :param mysql_config: dicionário com parâmetros do MySQL
                         { 'host':..., 'user':..., 'password':..., 'database':... }
    """

    # 1. Coleta os dados da URL
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Erro ao acessar {url}: {response.status_code}")

    # 2. Parseia o XML
    root = ET.fromstring(response.content)

    ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
    identify = root.find("oai:Identify", ns)

    if identify is None:
        raise Exception(
            "Não foi possível encontrar o elemento Identify no XML.")

    # Extrair informações principais
    repo_name = identify.find("oai:repositoryName", ns).text
    base_url = identify.find("oai:baseURL", ns).text
    protocol = identify.find("oai:protocolVersion", ns).text
    admin_email = identify.find("oai:adminEmail", ns).text
    earliest_date = identify.find("oai:earliestDatestamp", ns).text
    deleted_record = identify.find("oai:deletedRecord", ns).text
    granularity = identify.find("oai:granularity", ns).text

    # 3. Conectar ao banco MySQL
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    # Criar tabela se não existir
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS oai_identify (
            id INT AUTO_INCREMENT PRIMARY KEY,
            repository_name VARCHAR(255),
            base_url TEXT,
            protocol_version VARCHAR(50),
            admin_email VARCHAR(255),
            earliest_datestamp VARCHAR(50),
            deleted_record VARCHAR(50),
            granularity VARCHAR(50)
        )
    """)

    # Inserir dados
    cursor.execute(
        """
        INSERT INTO oai_identify
        (repository_name, base_url, protocol_version, admin_email, earliest_datestamp, deleted_record, granularity)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (repo_name, base_url, protocol, admin_email, earliest_date,
          deleted_record, granularity))

    conn.commit()
    cursor.close()
    conn.close()

    print("Dados do Identify armazenados no MySQL com sucesso!")


# Exemplo de uso:
if __name__ == "__main__":
    url = "https://seer.ufrgs.br/emquestao/oai?verb=Identify"
    mysql_config = database.config()

    coletar_identify_oai(url, mysql_config)
