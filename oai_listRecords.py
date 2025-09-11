import requests
import xml.etree.ElementTree as ET
import mysql.connector

import database

NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "dc": "http://purl.org/dc/elements/1.1/"
}


def coletar_registros_oai(base_url, mysql_config):
    """
    Coleta registros OAI-PMH (ListRecords) e armazena em MySQL em formato de Identify + triplas.
    """

    def processar_lote(xml_content, cursor):
        root = ET.fromstring(xml_content)

        for record in root.findall(".//oai:record", NS):
            header = record.find("oai:header", NS)

            oai_id = header.find("oai:identifier", NS).text if header.find(
                "oai:identifier", NS) is not None else None
            datestamp = header.find("oai:datestamp", NS).text if header.find(
                "oai:datestamp", NS) is not None else None
            setSpec = header.find("oai:setSpec", NS).text if header.find(
                "oai:setSpec", NS) is not None else None
            deleted = "status" in header.attrib and header.attrib[
                "status"] == "deleted"

            # Inserir na tabela oai_records
            cursor.execute(
                """
                INSERT INTO oai_records (oai_identifier, datestamp, setSpec, deleted)
                VALUES (%s, %s, %s, %s)
            """, (oai_id, datestamp, setSpec, deleted))
            record_id = cursor.lastrowid

            # Se não foi deletado, insere metadados como triplas
            if not deleted:
                metadata = record.find("oai:metadata", NS)
                if metadata is not None:
                    dc = metadata.find("dc:dc", NS)
                    if dc is not None:
                        for elem in dc:
                            tag = elem.tag.split("}")[
                                1]  # pega apenas o nome (ex: title, creator)
                            value = elem.text
                            if value:
                                # Mapeamento para propriedades
                                prop_map = {
                                    "title": "hasTitle",
                                    "creator": "hasAuthor",
                                    "subject": "hasSubject",
                                    "identifier": "hasURL",
                                    "date": "hasDate",
                                    "publisher": "hasPublisher",
                                    "language": "hasLanguage",
                                    "description": "hasDescription",
                                    "rights": "hasLicence"
                                }
                                prop = prop_map.get(tag,
                                                    f"has{tag.capitalize()}")
                                cursor.execute(
                                    """
                                    INSERT INTO oai_triples (record_id, property, value)
                                    VALUES (%s, %s, %s)
                                """, (record_id, prop, value))

        # Verificar se há resumptionToken
        token_elem = root.find(".//oai:resumptionToken", NS)
        return token_elem.text if token_elem is not None and token_elem.text.strip(
        ) else None

    # Conectar MySQL
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    url = f"{base_url}?verb=ListRecords&metadataPrefix=oai_dc"
    while url:
        print(f"🔎 Coletando: {url}")
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Erro ao acessar {url}: {response.status_code}")

        resumption_token = processar_lote(response.content, cursor)
        conn.commit()

        if resumption_token:
            url = f"{base_url}?verb=ListRecords&resumptionToken={resumption_token}"
        else:
            url = None

    cursor.close()
    conn.close()
    print("✅ Coleta concluída e dados armazenados no MySQL.")


# ---------------------------
# Exemplo de uso
# ---------------------------
if __name__ == "__main__":
    base_url = "https://seer.ufrgs.br/emquestao/oai"
    mysql_config = database.config()
    coletar_registros_oai(base_url, mysql_config)
