import mysql.connector
from collections import Counter

import database


def analisar_subjects(mysql_config, top_n=9999999):
    """
    Lê os subjects no MySQL, separa por ponto e vírgula,
    agrupa e mostra análise de frequência.

    :param mysql_config: dict {host, user, password, database}
    :param top_n: número de termos mais frequentes a mostrar
    """
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT subjects FROM oai_records WHERE subjects IS NOT NULL AND subjects != ''"
    )
    rows = cursor.fetchall()

    all_subjects = []
    for row in rows:
        subjects = row[0].split(";")
        for s in subjects:
            termo = s.strip().lower()
            if termo:
                all_subjects.append(termo)

    contador = Counter(all_subjects)

    print(f"\nTop {top_n} subjects mais frequentes:\n")
    for termo, freq in contador.most_common(top_n):
        print(f"{termo}: {freq}")

    cursor.close()
    conn.close()

    return contador


# Exemplo de uso
if __name__ == "__main__":
    mysql_config = database.config()

    analisar_subjects(mysql_config, top_n=999999)
