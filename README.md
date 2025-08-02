# Web Scraping Course

---

## Installing Python on Windows

1. Go to the official Python website:
   [https://www.python.org/downloads/](https://www.python.org/downloads/)

2. Download the latest Python version for Windows (e.g., Python 3.12.x or higher).

3. Run the downloaded installer.

4. **Important:**
   On the first installation screen, make sure to **check the box for "Add Python to PATH"**.
   This allows you to run the `python` command from PowerShell and CMD.

5. Click the **Install Now** button.

6. Wait for the installation to complete.

7. After installation finishes, open PowerShell or CMD and run the following command to verify Python is installed:

   ```powershell
   python --version
   ```

   You should see the Python version displayed, for example:

   ```
   Python 3.12.0
   ```


---

## Enabling Script Execution Policy for All Users on the System

To enable running PowerShell scripts for **all users**, you need to set the Execution Policy at the `LocalMachine` scope. This requires **Administrator privileges**.

### Steps to Enable Execution Policy for All Users

1. Open **PowerShell as Administrator**:

   * Click on the **Start** menu.
   * Type `powershell`.
   * Right-click on **Windows PowerShell** and select **Run as Administrator**.

2. In the opened PowerShell window, run the following command:

   ```powershell
   Set-ExecutionPolicy RemoteSigned -Scope LocalMachine
   ```

3. When prompted, type:

   ```
   Y
   ```

### Explanation

* `RemoteSigned` means:

  * Local scripts can run without restriction.
  * Scripts downloaded from the internet must be digitally signed.
* This setting applies to **all users on the system**.

### Security Notice

This setting lowers the system security slightly because local scripts can run without restriction.
If your system is used by multiple users or in sensitive environments, please consider this before applying the change.

---

### After applying the setting

Open a **new PowerShell window** (normal user mode) and activate your virtual environment by running:

```powershell
D:\web_scraping\web_scraping_course\.venv\Scripts\Activate.ps1
```

---

Sure! Here's a clean and professional version of those steps for your `README.md` file in English:

---

### 🛠️ Set up a Virtual Environment (Windows + PowerShell)

#### 1. Create a virtual environment

```powershell
py -m venv .venv
```

#### 2. Activate the virtual environment

```powershell
.\.venv\Scripts\Activate.ps1
```

You should see the virtual environment name like `(.venv)` appear in your terminal prompt.

#### 3. Install required packages

```powershell
pip install -r requirements.txt
```

---


