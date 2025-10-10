import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Load Excel file
file_path = "C:/Users/Admin/Downloads/Company Wise HR Contacts - HR Contacts.xlsx"
df = pd.read_excel(file_path)
# "C:/Users/Admin/Downloads/Company Wise HR Contacts - HR Contacts.xlsx"
# Email configuration
SMTP_SERVER = "smtp.gmail.com"  # Change if using another email provider
SMTP_PORT = 587
SENDER_EMAIL = "manikantasaivootla@gmail.com"
SENDER_PASSWORD = "mjgt ulda"  # Use App Password if using Gmail

# Subject and Message Template
subject = "Manikanta Sai || GCP Data Engineer 4.1 Years of Experience || Immediate Joiner|| Mobile: 9603071591"
message_template = """Dear {name},

I hope this message finds you well.

I am excited to apply for the GCP Data Engineer position. With 4.1 years of experience in building scalable data pipelines on (GCP) using technologies such as BigQuery,GCS, Airflow, Dataflow, Pub/Sub, Google cloud SDK, DataProc, Jenkins, Git and SQL/Python,PySpark and  BigData technologies such as Hadoop and Kafka. I am confident in my ability to contribute effectively to your team.

I have attached my updated resume for your reference and would be eager to discuss how I can contribute to your organization. I am available to join immediately.

Thank you for your time and consideration. I look forward to hearing from you.

Best regards,  
Manikanta Sai Vootla
M: 9014966629
"""

# Send emails
try:
    server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
    server.starttls()
    server.login(SENDER_EMAIL, SENDER_PASSWORD)

    for index, row in df.iterrows():
        recipient_email = row["Email"]
        recipient_name = row["Name"]

        msg = MIMEMultipart()
        msg["From"] = SENDER_EMAIL
        msg["To"] = recipient_email
        msg["Subject"] = subject

        message = message_template.format(name=recipient_name)
        msg.attach(MIMEText(message, "plain"))

        server.sendmail(SENDER_EMAIL, recipient_email, msg.as_string())
        print(f"Email sent to {recipient_name} ({recipient_email})")

    server.quit()
    print("All emails sent successfully!")

except Exception as e:
    print(f"Error: {e}")
