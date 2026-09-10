# Deploying FinGuard Live Demo to Streamlit Community Cloud (100% Free)

This guide shows you how to deploy the FinGuard Live Dashboard to a public URL (e.g. `https://finguard-demo.streamlit.app`) in **3 minutes** completely free with **no credit card required**.

---

## Why Streamlit Community Cloud for Interviews?
1. **Free Forever**: Hosted on cloud infrastructure at zero cost.
2. **Instant Boot**: Zero cluster spin-up lag during interviews (unlike Databricks compute which takes 4–5 minutes to wake up).
3. **Public Shareable Link**: You can paste this URL at the top of your resume, LinkedIn profile, and GitHub repository.
4. **Interactive Showcase**: Interviewers can click "Inject High-Value Fraud" or "Watchlist Match" live on their own phone or laptop.

---

## Step 1: Push Project to GitHub
1. Initialize Git in the project folder (if not already done):
   ```bash
   cd "c:\Users\ASUS\OneDrive - jainuniversity.ac.in\Projects\FinGuard\finguard_streaming_project-main"
   git init
   git add .
   git commit -m "FinGuard real-time fraud detection platform"
   ```
2. Create a new public or private repository on [GitHub](https://github.com/new) named `finguard-streaming-project`.
3. Push your repository:
   ```bash
   git branch -M main
   git remote add origin https://github.com/<your-username>/finguard-streaming-project.git
   git push -u origin main
   ```

---

## Step 2: Deploy on Streamlit Community Cloud
1. Go to [share.streamlit.io](https://share.streamlit.io) and log in with your GitHub account.
2. Click **New app**.
3. Select your repository: `<your-username>/finguard-streaming-project`.
4. Set the branch: `main`.
5. Set the Main file path: `dashboard/app.py`.
6. (Optional) In **Advanced settings**, set Python version to `3.11` or `3.12`.
7. Click **Deploy!**

In ~60 seconds, your app will be live with a public URL!

---

## Step 3: Add Free Cloud Secrets (Optional)
If you want the live app on Streamlit Cloud to connect to your real **Neon PostgreSQL** or **Gmail SMTP**:
1. In your Streamlit app dashboard, click **Settings** > **Secrets**.
2. Paste the environment variables:
   ```toml
   DATABASE_URL = "postgresql://user:password@ep-xyz.neon.tech/finguard?sslmode=require"
   EMAIL_FROM = "your_email@gmail.com"
   GMAIL_APP_PASSWORD = "xxxx xxxx xxxx xxxx"
   ```
3. Save. The app will automatically connect!

---

## Step 4: Resume & Interview Presentation
Include this bullet on your resume:
> **FinGuard — Real-Time Credit Card Fraud Detection Platform**  
> *Live Demo:* `https://finguard-demo.streamlit.app` | *GitHub:* `github.com/<username>/finguard`  
> - Engineered an end-to-end streaming fraud detection engine processing live transactions from Confluent Kafka and Auto Loader into a Medallion Delta Lake architecture on Databricks.
> - Formulated stream-static joins for credit threshold violations and 5-minute watermarked stream-stream joins for watchlist card matching.
> - Automated instant email notifications via Gmail SMTP with Databricks Secret Scopes and built a sub-second live monitoring dashboard.
