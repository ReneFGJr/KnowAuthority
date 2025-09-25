import requests
import xml.etree.ElementTree as ET
import mysql.connector
import database

NS = {
    "oai": "http://www.openarchives.org/OAI/2.0/",
    "dc": "http://purl.org/dc/elements/1.1/"
}


def coletar_registros_oai(base_url, mysql_config=database.config()):
    """
    Coleta registros OAI-PMH (ListRecords) e armazena em MySQL em formato de Identify + triplas.
    """

    def processar_lote(xml_content, cursor):
        root = ET.fromstring(xml_content)

        for record in root.findall(".//oai:record", NS):
            header = record.find("oai:header", NS)

            oai_id = header.find("oai:identifier", NS).text if header.find("oai:identifier", NS) is not None else None
            datestamp = header.find("oai:datestamp", NS).text if header.find("oai:datestamp", NS) is not None else None
            setSpec = header.find("oai:setSpec", NS).text if header.find("oai:setSpec", NS) is not None else None
            deleted = "status" in header.attrib and header.attrib["status"] == "deleted"

            # ⚡ Evitar duplicados: verificar se já existe esse oai_identifier
            cursor.execute("SELECT id FROM oai_records WHERE oai_identifier = %s", (oai_id,))
            existing = cursor.fetchone()

            if existing:
                record_id = existing[0]
                print(f"⚠️ Registro já existe: {oai_id}, id={record_id}")
            else:
                cursor.execute(
                    """
                    INSERT INTO oai_records (oai_identifier, datestamp, setSpec, deleted)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (oai_id, datestamp, setSpec, deleted)
                )
                record_id = cursor.lastrowid
                print(f"✅ Novo registro inserido: {oai_id}")

            # Se não foi deletado, insere metadados como triplas
            if not deleted and record_id:
                metadata = record.find("oai:metadata", NS)
                if metadata is not None:
                    dc = metadata.find("dc:dc", NS)
                    if dc is not None:
                        for elem in dc:
                            tag = elem.tag.split("}")[1]  # pega apenas o nome (ex: title, creator)
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
                                prop = prop_map.get(tag, f"has{tag.capitalize()}")

                                # ⚡ Evitar duplicados em triplas
                                cursor.execute(
                                    "SELECT id FROM oai_triples WHERE record_id = %s AND property = %s AND value = %s",
                                    (record_id, prop, value)
                                )
                                if not cursor.fetchone():
                                    cursor.execute(
                                        """
                                        INSERT INTO oai_triples (record_id, property, value)
                                        VALUES (%s, %s, %s)
                                        """,
                                        (record_id, prop, value)
                                    )

        # Verificar se há resumptionToken
        token_elem = root.find(".//oai:resumptionToken", NS)
        return token_elem.text if token_elem is not None and token_elem.text.strip() else None

    # 🔹 Conectar ao banco
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    # 🔹 Loop de coleta
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


def tables():
    mysql_config = database.config()
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    # 🔹 Criar tabelas se não existirem
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS oai_records (
            id INT AUTO_INCREMENT PRIMARY KEY,
            oai_identifier VARCHAR(255) UNIQUE,   -- 🔑 evita duplicados direto no banco
            datestamp VARCHAR(50),
            setSpec VARCHAR(255),
            deleted BOOLEAN DEFAULT 0
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS oai_triples (
            id INT AUTO_INCREMENT PRIMARY KEY,
            record_id INT,
            property VARCHAR(100),
            value TEXT,
            FOREIGN KEY (record_id) REFERENCES oai_records(id) ON DELETE CASCADE
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("🛠️ Tabelas criadas/verificadas.")


# ---------------------------
# Exemplo de uso
# ---------------------------
if __name__ == "__main__":
    tables()
    base_url = "https://seer.ufrgs.br/emquestao/oai"
    coletar_registros_oai(base_url)
