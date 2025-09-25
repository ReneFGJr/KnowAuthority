import requests
import xml.etree.ElementTree as ET
import mysql.connector
import database


def update_identify(data, mysql_config=database.config()):
    """
    Atualiza os dados do Identify no MySQL para um repositório já existente (UPDATE).
    :param data: dict com {id, base_url}
    :param mysql_config: conexão MySQL
    :return: dict com dados coletados
    """
    url = data.get("base_url")
    repo_id = data.get("id")

    if not url or not repo_id:
        raise Exception("Dados inválidos: é necessário id e base_url")

    # Garante verb=Identify
    if 'verb=Identify' not in url:
        if '?' in url:
            url += '&verb=Identify'
        else:
            url += '?verb=Identify'

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Erro ao acessar {url}: {response.status_code}")

    # Parseia o XML
    root = ET.fromstring(response.content)
    ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}
    identify = root.find("oai:Identify", ns)

    if identify is None:
        raise Exception("Não foi possível encontrar o elemento Identify no XML.")

    # Extrair dados
    repo_data = {
        "repositoryName": identify.find("oai:repositoryName", ns).text if identify.find("oai:repositoryName", ns) is not None else None,
        "baseURL": identify.find("oai:baseURL", ns).text if identify.find("oai:baseURL", ns) is not None else url,
        "protocolVersion": identify.find("oai:protocolVersion", ns).text if identify.find("oai:protocolVersion", ns) is not None else None,
        "adminEmail": identify.find("oai:adminEmail", ns).text if identify.find("oai:adminEmail", ns) is not None else None,
        "earliestDatestamp": identify.find("oai:earliestDatestamp", ns).text if identify.find("oai:earliestDatestamp", ns) is not None else None,
        "deletedRecord": identify.find("oai:deletedRecord", ns).text if identify.find("oai:deletedRecord", ns) is not None else None,
        "granularity": identify.find("oai:granularity", ns).text if identify.find("oai:granularity", ns) is not None else None,
    }

    # Atualizar no banco
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    update_query = """
        UPDATE oai_identify
        SET repository_name = %s,
            base_url = %s,
            protocol_version = %s,
            admin_email = %s,
            earliest_datestamp = %s,
            deleted_record = %s,
            granularity = %s
        WHERE id = %s
    """
    cursor.execute(update_query, (
        repo_data["repositoryName"],
        repo_data["baseURL"],
        repo_data["protocolVersion"],
        repo_data["adminEmail"],
        repo_data["earliestDatestamp"],
        repo_data["deletedRecord"],
        repo_data["granularity"],
        repo_id
    ))
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Dados do Identify atualizados com sucesso para id={repo_id}")
    return repo_data
