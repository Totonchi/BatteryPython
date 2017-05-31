import smtplib
from email.mime.text import MIMEText

msg = 'Hello World'
me = 'devuser@stromcore.tk'
you = 'test@mailinator.com'
email = MIMEText(msg)
email['Subject'] = "Testing, testing"
email['From'] = me 
email['To'] = you

s = smtplib.SMTP('localhost')
s.sendmail(me, [you], email.as_string())
s.quit() 
