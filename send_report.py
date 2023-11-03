import base64
import json
import datetime
from dotenv import load_dotenv
import schedule
import time
import psycopg2
from jinja2 import Environment, FileSystemLoader
import requests
import os
from email.mime.multipart import MIMEMultipart

load_dotenv()
os.environ["TZ"] = "Asia/Almaty"


# time.tzset()

# GET DATABASE CONNECTION
def get_database_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        host=os.getenv("HOST"),
        port=os.getenv("PORT")
    )


# FUNCTION SEND REPORT
def send_report():
    conn = get_database_connection()
    cur = conn.cursor()

    # QUERIES
    queries = [
        """
    SELECT
     DATE_TRUNC('day', ("createdAt" + INTERVAL '6 hours')) AS "payment_day",
     SUM("price") AS "total_payment_amount"
    FROM
     "payment"
    WHERE
     "status" = 'paid'
     AND DATE_TRUNC('day', ("createdAt" + INTERVAL '6 hours')) = DATE_TRUNC('day', CURRENT_TIMESTAMP - INTERVAL '8 day')
    GROUP BY
     "payment_day"
    ORDER BY
     "payment_day" DESC;
        """,
        """
    SELECT
     DATE_TRUNC('month', ("createdAt" + INTERVAL '6 hours')) AS "payment_month",
     SUM("price") AS "total_payment_amount"
    FROM
     "payment"
    WHERE
     "status" = 'paid'
     AND DATE_TRUNC('month', ("createdAt" + INTERVAL '6 hours')) = DATE_TRUNC('month', CURRENT_TIMESTAMP) - INTERVAL '1 month'
    GROUP BY
     "payment_month"
    ORDER BY
     "payment_month" DESC;
        """,
        """
    SELECT
     DATE_TRUNC('day', u."createdAt" + INTERVAL '6 hours') AS "day",
     COUNT(DISTINCT u."id") AS "total_register",
     COUNT(DISTINCT r."id") AS "total_reports",
     COUNT(DISTINCT CASE WHEN r."status" = 'not_paid' THEN r."id" END) AS "free_reports",
     COUNT(DISTINCT CASE WHEN r."status" = 'paid' THEN r."id" END) AS "paid_reports"
    FROM
        "user" u
    LEFT JOIN
        "payment" r ON u."id" = r."userId"
    WHERE
        DATE_TRUNC('day', u."createdAt" + INTERVAL '6 hours') = DATE_TRUNC('day', CURRENT_TIMESTAMP - INTERVAL '8 day')
    GROUP BY
        "day"
    ORDER BY
        "day" DESC;
        """,
        """
    SELECT
     COUNT(DISTINCT u."id") AS "total_register",
     COUNT(DISTINCT r."id") AS "total_reports",
     COUNT(DISTINCT CASE WHEN r."status" = 'not_paid' THEN r."id" END) AS "free_reports",
     COUNT(DISTINCT CASE WHEN r."status" = 'paid' THEN r."id" END) AS "paid_reports"
    FROM
        "user" u
    LEFT JOIN
        "payment" r ON u."id" = r."userId";
        """,
        """
    SELECT
     DATE_TRUNC('day', r."createdAt" + INTERVAL '6 hours') AS "day",
     COUNT(DISTINCT r."id") AS "total_reports",
     COUNT(DISTINCT CASE WHEN p."status" = 'not_paid' THEN r."id" END) AS "free_reports",
     COUNT(DISTINCT CASE WHEN p."status" = 'paid' THEN r."id" END) AS "paid_reports"
    FROM
        "report" r
    LEFT JOIN
        "payment" p ON r."userId" = p."userId"
    WHERE
        r."status" = 'done'
        AND DATE_TRUNC('day', r."createdAt" + INTERVAL '6 hours') = DATE_TRUNC('day', CURRENT_TIMESTAMP - INTERVAL '8 day')
    GROUP BY
        "day"
    ORDER BY
        "day" DESC;
        """,
        """
    SELECT
     MIN(DATE_TRUNC('day', r."createdAt" + INTERVAL '6 hours')) AS "earliest_report_date",
     COUNT(DISTINCT r."id") AS "total_reports",
     COUNT(DISTINCT CASE WHEN p."status" = 'not_paid' THEN r."id" END) AS "free_reports",
     COUNT(DISTINCT CASE WHEN p."status" = 'paid' THEN r."id" END) AS "paid_reports"
    FROM
        "report" r
    LEFT JOIN
        "payment" p ON r."userId" = p."userId"
    WHERE
        r."status" = 'done';
        """,
    ]

    # FORMATTED RESULTS
    formatted_results = []
    for query in queries:
        cur.execute(query)
        result = cur.fetchall()
        for record in result:
            key_name1 = "date"
            key_name2 = "sum"

            if isinstance(record[0], datetime.date):
                date_str = record[0].strftime('%Y-%m-%d')
            else:
                date_str = str(record[0])
                key_name1 = "sum"
                key_name2 = "report_sum"

            record_dict = {key_name1: date_str, key_name2: record[1]}
            if len(record) > 2:
                record_dict["not_paid"] = record[2]
            if len(record) > 3:
                record_dict["paid"] = record[3]
            formatted_results.append(record_dict)

    print(formatted_results)
    cur.close()
    conn.close()

    # GET ACCESS TOKEN
    global access_token
    response = requests.post(os.getenv("SENDPULSE_API_URL") + '/oauth/access_token', {
        'grant_type': os.getenv("SENDPULSE_GRANT_TYPE"),
        'client_id': os.getenv("SENDPULSE_CLIENT_ID"),
        'client_secret': os.getenv("SENDPULSE_CLIENT_SECRET")
    })

    if response.status_code == 200:
        access_token = json.loads(response.text)['access_token']
        print("Access Token:", 'Received!')
    else:
        print("Error receiving token:", response.status_code, response.text)

    # CREATION TEMPLATES
    file_loader = FileSystemLoader('templates')
    env = Environment(loader=file_loader)
    template = env.get_template('report_template.html')

    yesterday_payments = formatted_results[0]
    last_month_payments = formatted_results[1]
    yesterday_users = formatted_results[2]
    total_users = formatted_results[3]
    yesterday_reports = formatted_results[4]
    total_reports = formatted_results[5]

    html_content = template.render({
        'yesterday_payments': yesterday_payments,
        'last_month_payments': last_month_payments,
        'yesterday_users': yesterday_users,
        'total_users': total_users,
        'yesterday_reports': yesterday_reports,
        'total_reports': total_reports
    })

    # SEND REPORT TO EMAIL
    subject = 'Report'
    recipients = ['kvg-1999@mail.ru']
    text_content = 'Ежедневный отчет'
    from_email = os.getenv('FROM_EMAIL')
    email = MIMEMultipart('alternative')
    email['Subject'] = subject
    email['From'] = from_email

    encoded_html_content = base64.b64encode(html_content.encode('utf-8')).decode('utf-8')

    response = requests.post(
        os.getenv("SENDPULSE_API_URL") + '/smtp/emails',
        json={
            'email': {
                'html': encoded_html_content,
                'text': text_content,
                'subject': subject,
                'from': {
                    'name': 'Vintex',
                    'email': from_email
                },
                'to': [{'email': addr} for addr in recipients]
            }
        },
        headers={
            'Authorization': f'Bearer {access_token}'
        }
    )

    if response.status_code == 200:
        print("Письмо успешно отправлено")
    else:
        print("Ошибка отправки: ", response.status_code, response.text)


# CALLING A FUNCTION
send_report()

# TIMER
schedule.every().day.at("09:00").do(send_report)

while True:
    schedule.run_pending()
    time.sleep(1)
