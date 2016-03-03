#!/usr/bin/env python

import smtplib
import sys

if  __name__ == '__main__':
  fromaddr = 'qqss88@gmail.com'
  toaddrs  = 'charliesun+sgwuigdlacgn5gr4iboh@boards.trello.com'
  msg = sys.argv[1]


# Credentials (if needed)
  name_pwd = open('.gmail_pwd','r').read().strip()
  username = name_pwd.split('::')[0]
  password = name_pwd.split('::')[1]

# The actual mail send
  server = smtplib.SMTP('smtp.gmail.com:587')
  server.starttls()
  server.login(username,password)
  server.sendmail(fromaddr, toaddrs, msg)
  print 'Emailed to trello. Done.'
  server.quit()
