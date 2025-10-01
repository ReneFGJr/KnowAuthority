# oai_listIdentifiers.py
import requests
import xml.etree.ElementTree as ET
import mysql.connector
import database

NS = {"oai": "http://www.openarchives.org/OAI/2.0/"}


def coletar_identificadores(base_url, identify_id):
    conn = mysql.connector.connect(**database.config())
    cursor = conn.cursor()

    total = 0
    ignorados = 0
    params = {"verb": "ListIdentifiers", "metadataPrefix": "oai_dc"}

    while True:
        resp = requests.get(base_url, params=params, timeout=30)
        print("==>",resp.url)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)

        for header in root.findall(".//oai:header", NS):
            oai_id = header.find("oai:identifier", NS).text if header.find(
                "oai:identifier", NS) is not None else None
            datestamp = header.find("oai:datestamp", NS).text if header.find(
                "oai:datestamp", NS) is not None else None
            setSpec = header.find("oai:setSpec", NS).text if header.find(
                "oai:setSpec", NS) is not None else None
            deleted = 1 if "status" in header.attrib and header.attrib[
                "status"] == "deleted" else 0

            cursor.execute(
                """
                INSERT IGNORE INTO oai_records
                (repository, oai_identifier, datestamp, setSpec, deleted)
                VALUES (%s, %s, %s, %s, %s)
            """, (identify_id, oai_id, datestamp, setSpec, deleted))

            if cursor.rowcount > 0:
                total += 1
            else:
                ignorados += 1

        # --- Trata resumptionToken
        token = root.find(".//oai:resumptionToken", NS)
        if token is not None and token.text:
            params = {
                "verb": "ListIdentifiers",
                "resumptionToken": token.text.strip()
            }
        else:
            break

    conn.commit()
    conn.close()
    return {"inseridos": total, "ignorados": ignorados}
