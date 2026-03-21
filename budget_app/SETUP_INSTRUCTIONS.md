# Budget Generator Setup Instructions

**For Century Management staff who need to run the Budget Generator web app**

---

## What You Need

Before you start, make sure you have:
- A Mac computer
- Google Drive synced on your computer
- Chrome browser
- Your Yardi Voyager login credentials

---

## Step 1: Check if Python is Installed

Python is a programming language your computer needs to run the Budget Generator.

1. Open **Terminal**
   - Press **Cmd + Spacebar** to open Spotlight Search
   - Type "Terminal" and press Enter

2. Copy and paste this command, then press Enter:
   ```
   python3 --version
   ```

3. Look at what appears:
   - **If you see "Python 3.x.x"** → You're all set! Skip to Step 2.
   - **If you see "command not found"** → Go to the "Install Python" section below.

### Install Python (if needed)

1. Open Chrome and go to **python.org**
2. Look for the big yellow **"Download"** button and click it
3. Click the installer for Mac (it will say something like "macOS 64-bit installer")
4. Wait for the file to download, then double-click it
5. Follow the instructions that pop up (just keep clicking "Next" until it's done)
6. When finished, open Terminal again and run:
   ```
   python3 --version
   ```
   You should now see a version number.

---

## Step 2: Install Required Software Libraries

The Budget Generator needs two software libraries: Flask and openpyxl.

1. Open **Terminal** (if not already open)

2. Copy and paste this command, then press Enter:
   ```
   pip3 install flask openpyxl
   ```

3. Wait for it to finish (you'll see a bunch of text scroll by). When it's done, you're ready to go.

---

## Step 3: Open the Budget Generator App

1. Open **Finder** (the smiley face icon on your dock)

2. In the left sidebar, click **"Google Drive"** or search for it

3. Navigate to: **Claude > Claude Work > Projects > Budgets > budget_app**
   - If you don't see these folders, check that your Google Drive is synced

4. In the **budget_app** folder, you should see a file called `app.py`. Right-click on the folder and select **"Copy"** (you'll need the path in a moment)

5. Open **Terminal**

6. Type this and stop (don't press Enter yet):
   ```
   cd
   ```
   (with a space at the end)

7. Go back to **Finder**, drag the **budget_app** folder into the Terminal window

8. Press **Enter**
   - Your Terminal should now show something like: `your-username:budget_app username$`

9. Copy and paste this command, then press Enter:
   ```
   python3 app.py
   ```

10. You should see messages appear that say something like "Running on http://127.0.0.1:5000"

11. Open **Chrome** and go to: **http://127.0.0.1:5000**
    - You should see the Budget Generator web app!

**Leave Terminal open while you use the app.** When you're done, press **Ctrl + C** in Terminal to stop it.

---

## How to Use the Budget Generator

The Budget Generator works in two steps:

### Step 1: Download YSL Reports from Yardi

1. In the Budget Generator web app, **select which buildings** you need budgets for

2. **Enter your Yardi email** (your Century Management email address)

3. **Enter the budget period** in this format:
   - Example: `January-2027` or `December-2026`
   - Make sure the month is spelled out (not abbreviated)

4. Click **"Generate Console Script"**

5. **Copy the script** that appears (Cmd + A to select all, then Cmd + C to copy)

6. Open **Chrome** and go to **Yardi Voyager**
   - Make sure you're logged in to your account

7. Press **Cmd + Option + J** to open the Developer Console
   - A black box should appear at the bottom of your screen

8. **Paste the script** you copied (Cmd + V)

9. Press **Enter** and wait
   - You'll see a lot of activity in the console
   - When it's done, YSL files will download to your Mac (you'll see notifications in the top right)

### Step 2: Generate the Budgets

1. Back in the Budget Generator web app, drag your downloaded **YSL files** into the upload area
   - Or click to browse and select them

2. Click **"Generate Budgets"**

3. Wait for it to finish
   - Your completed budget files will download to your Mac
   - They'll also be saved to the folder shown in the app's **Settings**

---

## Troubleshooting

### "This site can't be reached" or "localhost denied"

Try using this address instead of `127.0.0.1:5000`:
```
http://127.0.0.1:5000
```

### "command not found: python3"

Use this instead:
```
python3 app.py
```

(Make sure you typed it exactly as shown above)

### "command not found: pip3"

Use this instead:
```
pip3 install flask openpyxl
```

### The app won't start

1. Make sure you're in the **budget_app** folder (check your Terminal prompt)
2. Try the command again:
   ```
   python3 app.py
   ```

### YSL download fails or doesn't start

1. Check that you're still **logged into Yardi Voyager** in Chrome
2. Yardi sessions timeout after a while, so you may need to log in again
3. Try the console script again from the beginning

### I don't see the budget_app folder in Google Drive

1. Make sure your **Google Drive is synced** on your Mac
2. Check the Google Drive app in your menu bar (top right)
3. If it's grayed out or says "Syncing," wait for it to finish
4. Try looking for the folder again

### Still stuck?

Take a screenshot of the error message and send it to your manager or the tech team.

---

## When You're Done

Press **Ctrl + C** in Terminal to stop the Budget Generator. It's safe to close Terminal after that.

