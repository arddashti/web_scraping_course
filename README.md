# TSETMC API
---

## 1️⃣ Installing Python on Windows

1. Go to the official Python website:  
   [https://www.python.org/downloads/](https://www.python.org/downloads/)

2. Download the latest Python version for Windows (e.g., Python 3.12.x or higher).

3. Run the downloaded installer.

4. **Important:** On the first installation screen, **check the box "Add Python to PATH"**.  
   This allows you to run `python` from PowerShell or CMD.

5. Click **Install Now** and wait for the installation to complete.

6. Verify Python installation:

   ```powershell
   python --version
Expected output:


Python 3.12.0
2️⃣ Enabling Script Execution Policy for All Users (PowerShell)
To run PowerShell scripts for all users, set the Execution Policy at the LocalMachine scope (Administrator required).

Steps
Open PowerShell as Administrator:

Start → type powershell → Right-click → Run as Administrator


Set-ExecutionPolicy RemoteSigned -Scope LocalMachine
When prompted, type:

powershell
Copy
Edit
Y
Explanation
RemoteSigned allows:

Local scripts to run without restriction.

Internet-downloaded scripts must be signed.

Applies to all users.

Security Notice
Local scripts can now run freely; consider the risk on shared or sensitive systems.

3️⃣ Set Up Virtual Environment (Windows + PowerShell)
Step 1: Create virtual environment

py -m venv .venv
Step 2: Activate virtual environment

.\.venv\Scripts\Activate.ps1
You should see (.venv) in your terminal prompt.

Step 3: Install required packages

pip install -r requirements.txt
Or manually:


pip install sqlalchemy pyodbc pandas requests lxml python-dotenv
4️⃣ Project Setup & Environment Variables
This project fetches TSETMC stock data via SOAP API and stores it in SQL Server.

4.1 Create .env file
In the project root:


# Database
DB_SERVER=localhost
DB_NAME=test
DB_USER=sa
DB_PASS=Ada@20215

# TSETMC API
TSETMC_USERNAME=novinib.com
TSETMC_PASSWORD=n07!1\1!13.Com04
TSETMC_URL=http://service.tsetmc.com/webservice/TsePublicV2.asmx
⚠️ Security: Do not commit .env to Git. Add it to .gitignore.

4.2 Load environment variables in Python

from dotenv import load_dotenv
import os

load_dotenv()

# Database
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# TSETMC API
TSETMC_USERNAME = os.getenv("TSETMC_USERNAME")
TSETMC_PASSWORD = os.getenv("TSETMC_PASSWORD")
TSETMC_URL = os.getenv("TSETMC_URL")
5️⃣ Initialize Database
Use init_db.py to create the schema and tables (checks existence before creating):

python src/init_db.py
Tables Created
instrument

adj_price_all

adj_price

board

TradeOneDay

All tables are under schema defined in TSETMC_SCHEMA (e.g., tsetmc_api).

6️⃣ Fetch Data from TSETMC API
Example SOAP request:

import requests
from lxml import etree

headers = {
    "Content-Type": "text/xml; charset=utf-8",
    "SOAPAction": '"http://tsetmc.com/Instrument"'
}

soap_body = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <Instrument xmlns="http://tsetmc.com/">
      <UserName>{TSETMC_USERNAME}</UserName>
      <Password>{TSETMC_PASSWORD}</Password>
      <Flow>1</Flow>
    </Instrument>
  </soap:Body>
</soap:Envelope>"""

response = requests.post(TSETMC_URL, data=soap_body.encode('utf-8'), headers=headers)
root = etree.fromstring(response.content)
# parse response as needed
7️⃣ Notes
Make sure SQL Server is running and .env credentials are correct.

.env allows easy switching of databases or credentials.

python-dotenv keeps environment variables secure and separate from code.

After setting the Execution Policy, always open a new PowerShell window to activate .venv:


D:\web_scraping\web_scraping_course\.venv\Scripts\Activate.ps1
✅ Now your development environment is fully configured and ready for web scraping and fetching data from TSETMC.


---

If you want, I can also create a **workflow diagram** showing `.env → DB → TSETMC API → Tables` to make the README visually more beginner-friendly.  

Do you want me to add that diagram?