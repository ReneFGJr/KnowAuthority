import requests
import xml.etree.ElementTree as ET
import mysql.connector

import database


def coletar_list_sets(base_url, identify_id, mysql_config=database.config()):
    """
    Coleta os conjuntos (sets) de um endpoint OAI-PMH e armazena no MySQL.

    :param base_url: URL base do OAI-PMH (ex: https://seer.ufrgs.br/emquestao/oai)
    :param identify_id: ID da tabela oai_identify que representa a fonte
    :param mysql_config: dicionário {host, user, password, database}
    """

    # Conectar ao banco
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor(dictionary=True)

    # Criar tabela de sets se não existir
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS oai_sets (
            id INT AUTO_INCREMENT PRIMARY KEY,
            identify_id INT,
            set_spec VARCHAR(255),
            set_name TEXT,
            set_description TEXT,
            FOREIGN KEY (identify_id) REFERENCES oai_identify(id),
            UNIQUE KEY uniq_identify_spec (identify_id, set_spec) -- 🔑 garante unicidade
        )
    """)

    # Requisição
    params = {"verb": "ListSets"}
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        print("Erro ao acessar:", response.status_code)
        return

    root = ET.fromstring(response.content)
    ns = {"oai": "http://www.openarchives.org/OAI/2.0/"}

    for set_elem in root.findall(".//oai:set", ns):
        set_spec = set_elem.find("oai:setSpec", ns).text if set_elem.find("oai:setSpec", ns) is not None else None
        set_name = set_elem.find("oai:setName", ns).text if set_elem.find("oai:setName", ns) is not None else None
        set_desc = None

        desc_elem = set_elem.find("oai:setDescription", ns)
        if desc_elem is not None:
            set_desc = ET.tostring(desc_elem, encoding="unicode")

        # --- Verificar se já existe antes de inserir
        cursor.execute(
            "SELECT id FROM oai_sets WHERE identify_id = %s AND set_spec = %s",
            (identify_id, set_spec)
        )
        existing = cursor.fetchone()

        if existing:
            print(f"Set {set_spec} já existe para identify_id {identify_id}, não inserido.")
        else:
            cursor.execute(
                """
                INSERT INTO oai_sets (identify_id, set_spec, set_name, set_description)
                VALUES (%s, %s, %s, %s)
                """,
                (identify_id, set_spec, set_name, set_desc)
            )
            conn.commit()
            print(f"Set {set_spec} inserido com sucesso para identify_id {identify_id}.")

    cursor.close()
    conn.close()
    print("Coleta de conjuntos concluída.")



# Exemplo de uso
if __name__ == "__main__":
    mysql_config = database.config()

    # suponha que o Identify deste repositório já esteja na tabela e tenha ID = 1
    coletar_list_sets(base_url="https://seer.ufrgs.br/emquestao/oai",
                      identify_id=1,
                      mysql_config=mysql_config)
