# Google Sheets Export Setup Guide

This comprehensive guide will walk you through setting up Google Sheets export functionality for the AmoCRM Data Exporter.

## Overview

The Google Sheets export feature allows you to export your AmoCRM data directly to Google Sheets with real-time updates, custom formatting, and comprehensive error handling. This setup process involves:

1. Setting up a Google Cloud project
2. Creating OAuth credentials
3. Creating Google Sheets for your data
4. Configuring environment variables
5. Testing the connection

**Estimated Setup Time:** 30-45 minutes
**Difficulty Level:** Medium

## Prerequisites

- Google account with access to Google Cloud Console
- AmoCRM Data Exporter installed and configured
- Basic familiarity with environment variables and configuration files

## Step-by-Step Setup

### Step 1: Google Cloud Console Setup (10 minutes)

#### 1.1 Create or Select a Project

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Sign in with your Google account
3. Either:
   - **Create a new project:** Click "Select a project" → "New Project" → Enter project name → "Create"
   - **Use existing project:** Click "Select a project" → Choose your project

#### 1.2 Enable Google Sheets API

1. In the Google Cloud Console, navigate to **"APIs & Services" > "Library"**
2. Search for **"Google Sheets API"**
3. Click on the Google Sheets API result
4. Click **"Enable"** button
5. Wait for the API to be enabled (may take a few minutes)

**✅ Verification:** You should see "API enabled" status and be able to access the API overview page.

### Step 2: OAuth Credentials Creation (15 minutes)

#### 2.1 Configure OAuth Consent Screen

1. In Google Cloud Console, go to **"APIs & Services" > "OAuth consent screen"**
2. Choose **"External"** user type (unless you have Google Workspace)
3. Click **"Create"**
4. Fill in the required information:
   - **App name:** "AmoCRM Data Exporter" (or your preferred name)
   - **User support email:** Your email address
   - **Developer contact information:** Your email address
5. Click **"Save and Continue"**
6. On the "Scopes" page, click **"Save and Continue"** (no changes needed)
7. On the "Test users" page:
   - Click **"Add Users"**
   - Add your email address
   - Click **"Save and Continue"**
8. Review the summary and click **"Back to Dashboard"**

#### 2.2 Create OAuth Client ID

1. Go to **"APIs & Services" > "Credentials"**
2. Click **"Create Credentials" > "OAuth client ID"**
3. Choose **"Desktop app"** as the application type
4. Enter a name: "AmoCRM Exporter Desktop Client"
5. Click **"Create"**
6. In the popup dialog:
   - Click **"Download JSON"**
   - Save the file as `credentials.json` in your project root directory
   - Click **"OK"**

**✅ Verification:** You should have a `credentials.json` file in your project root with the following structure:
```json
{
  "installed": {
    "client_id": "your-client-id",
    "client_secret": "your-client-secret",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    ...
  }
}
```

### Step 3: Create Google Sheets (10 minutes)

You need to create separate Google Sheets for each entity type you want to export.

#### 3.1 Create Spreadsheets

1. Go to [Google Sheets](https://sheets.google.com/)
2. Create the following spreadsheets:

   **For Leads/Deals:**
   - Click **"Blank"** to create a new spreadsheet
   - Rename it to "AmoCRM Leads Export"
   - Copy the spreadsheet ID from the URL (the long string between `/d/` and `/edit`)
   - Example URL: `https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit`
   - Spreadsheet ID: `1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms`

   **For Contacts:**
   - Create another new spreadsheet
   - Rename it to "AmoCRM Contacts Export"
   - Copy the spreadsheet ID

   **For Companies:**
   - Create another new spreadsheet
   - Rename it to "AmoCRM Companies Export"
   - Copy the spreadsheet ID

   **For Events (Optional):**
   - Create another new spreadsheet
   - Rename it to "AmoCRM Events Export"
   - Copy the spreadsheet ID

#### 3.2 Verify Access

Make sure all spreadsheets are:
- Accessible to your Google account
- Not restricted by sharing settings
- Have the correct permissions (you should be the owner)

**✅ Verification:** You should be able to open each spreadsheet and edit it without any permission errors.

### Step 4: Configure Environment Variables (5 minutes)

#### 4.1 Update .env File

1. Open your `.env` file (create one if it doesn't exist in your project root)
2. Add the following lines with your actual spreadsheet IDs:

```env
# Google Sheets Configuration
GOOGLE_SHEETS_LEADS_ID=your_leads_spreadsheet_id_here
GOOGLE_SHEETS_CONTACTS_ID=your_contacts_spreadsheet_id_here
GOOGLE_SHEETS_COMPANIES_ID=your_companies_spreadsheet_id_here
GOOGLE_SHEETS_EVENTS_ID=your_events_spreadsheet_id_here
```

3. Replace `your_*_spreadsheet_id_here` with the actual IDs you copied in Step 3
4. Save the `.env` file

#### 4.2 Example Configuration

```env
# Example .env file
GOOGLE_SHEETS_LEADS_ID=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms
GOOGLE_SHEETS_CONTACTS_ID=1AbCdEfGhIjKlMnOpQrStUvWxYz1234567890AbCdEf
GOOGLE_SHEETS_COMPANIES_ID=1ZyXwVuTsRqPoNmLkJiHgFeDcBa0987654321ZyXwVu
GOOGLE_SHEETS_EVENTS_ID=1QwErTyUiOpAsDfGhJkLzXcVbNm1234567890QwErTy
```

**✅ Verification:** Your `.env` file should contain all four spreadsheet ID variables with valid 44-character IDs.

### Step 5: Test Authentication (5 minutes)

#### 5.1 Run Configuration Validation

1. Restart your application to load the new environment variables
2. Run the configuration validation tool:

```bash
# Using the CLI tool
python -m amocrm_exporter.cli.config_validator_cli --report

# Or using the setup wizard
python -m amocrm_exporter.cli.setup_wizard_cli --status
```

#### 5.2 Complete OAuth Flow

1. When prompted, a browser window will open
2. Sign in to your Google account (if not already signed in)
3. You may see a warning "This app isn't verified":
   - Click **"Advanced"**
   - Click **"Go to AmoCRM Data Exporter (unsafe)"**
4. Review the permissions and click **"Allow"**
5. You should see a success message

**✅ Verification:** The validation tool should report successful authentication and spreadsheet access.

### Step 6: Final Validation (5 minutes)

#### 6.1 Run Comprehensive Validation

```bash
python -m amocrm_exporter.cli.config_validator_cli --report
```

#### 6.2 Test Export Functionality

1. Access the web interface
2. Go to the configuration validation page: `http://localhost:8000/config-validation`
3. Click **"Run Full Validation"**
4. Verify all tests pass
5. Test a small export to ensure data appears in your spreadsheets

**✅ Verification:** All validation tests should pass, and you should be able to export data to Google Sheets successfully.

## Using the Setup Tools

### Command Line Tools

#### Configuration Validator
```bash
# Run comprehensive validation
python -m amocrm_exporter.cli.config_validator_cli --report

# Test specific components
python -m amocrm_exporter.cli.config_validator_cli --test connection
python -m amocrm_exporter.cli.config_validator_cli --test oauth

# Get JSON output
python -m amocrm_exporter.cli.config_validator_cli --json
```

#### Setup Wizard
```bash
# Interactive setup wizard
python -m amocrm_exporter.cli.setup_wizard_cli --interactive

# Check setup status
python -m amocrm_exporter.cli.setup_wizard_cli --status

# Get help for specific step
python -m amocrm_exporter.cli.setup_wizard_cli --step 2

# Show troubleshooting guide
python -m amocrm_exporter.cli.setup_wizard_cli --troubleshooting
```

### Web Interface

Access the configuration tools through the web interface:

- **Configuration Validation:** `http://localhost:8000/config-validation`
- **Setup Status:** Available through the API at `/api/config/setup/status`

## Troubleshooting

### Common Issues

#### 1. "Credentials file not found"

**Symptoms:**
- Error: `credentials.json not found`
- Authentication fails immediately

**Solutions:**
- Download `credentials.json` from Google Cloud Console
- Ensure the file is in your project root directory
- Check that the filename is exactly `credentials.json`

#### 2. "This app isn't verified"

**Symptoms:**
- Warning screen during OAuth flow
- "This app isn't verified" message

**Solutions:**
- Click "Advanced" then "Go to [App Name] (unsafe)"
- Add your email as a test user in OAuth consent screen
- This is normal for development applications

#### 3. "Access denied to spreadsheet"

**Symptoms:**
- HTTP 403 Forbidden errors
- Cannot access spreadsheet

**Solutions:**
- Verify spreadsheet is shared with your Google account
- Ensure you have Editor permissions
- Check that spreadsheet ID is correct
- Confirm spreadsheet hasn't been deleted

#### 4. "Invalid spreadsheet ID format"

**Symptoms:**
- Configuration validation fails
- Invalid ID format errors

**Solutions:**
- Spreadsheet IDs must be exactly 44 characters
- Copy ID from the URL between `/d/` and `/edit`
- Don't include extra characters or spaces

#### 5. "Environment variables not loading"

**Symptoms:**
- Spreadsheet IDs showing as None
- Configuration validation fails

**Solutions:**
- Restart application after updating `.env` file
- Check `.env` file is in project root
- Verify variable names match exactly
- Ensure no spaces around `=` in `.env` file

### Getting Help

#### Diagnostic Commands

Run these commands to diagnose issues:

```bash
# Comprehensive diagnostic report
python -m amocrm_exporter.cli.config_validator_cli --report

# Test network connectivity
python -m amocrm_exporter.cli.config_validator_cli --test connection

# Validate OAuth setup
python -m amocrm_exporter.cli.config_validator_cli --test oauth

# Interactive troubleshooting
python -m amocrm_exporter.cli.setup_wizard_cli --troubleshooting
```

#### Log Files

Check application logs for detailed error information:
- Location: `logs/` directory
- Look for entries with `sheets_config` or `config_validator` tags

#### Support Resources

- [Google Sheets API Documentation](https://developers.google.com/sheets/api)
- [OAuth 2.0 Setup Guide](https://developers.google.com/identity/protocols/oauth2)
- [Google Cloud Console](https://console.cloud.google.com/)

## Security Considerations

### Credential Management

- Keep `credentials.json` secure and don't commit it to version control
- The `token.json` file contains your access token - treat it as sensitive
- Regularly review and rotate credentials if needed

### Spreadsheet Permissions

- Only share spreadsheets with necessary users
- Use Editor permissions (not Owner) for service accounts if applicable
- Regularly audit spreadsheet access

### API Quotas

- Google Sheets API has usage quotas
- Monitor your usage in Google Cloud Console
- Implement appropriate retry logic (already included in the exporter)

## Advanced Configuration

### Custom Batch Sizes

You can configure batch sizes for large exports:

```env
# Optional: Custom batch sizes
GOOGLE_SHEETS_BATCH_SIZE=1000
GOOGLE_SHEETS_MAX_RETRIES=3
```

### Multiple Environments

For different environments (development, staging, production):

```env
# Development
GOOGLE_SHEETS_LEADS_ID=dev_spreadsheet_id
GOOGLE_SHEETS_CONTACTS_ID=dev_contacts_id

# Production
GOOGLE_SHEETS_LEADS_ID=prod_spreadsheet_id
GOOGLE_SHEETS_CONTACTS_ID=prod_contacts_id
```

## Maintenance

### Regular Tasks

1. **Monitor API Usage:** Check Google Cloud Console for quota usage
2. **Update Credentials:** Refresh OAuth tokens as needed (automatic)
3. **Backup Spreadsheets:** Regular backups of important data
4. **Review Permissions:** Periodic audit of spreadsheet access

### Updates and Changes

When updating the exporter:
1. Check for new configuration requirements
2. Run validation after updates
3. Test export functionality
4. Review any new features or changes

## Conclusion

Once setup is complete, you'll have:

- ✅ Automated Google Sheets export functionality
- ✅ Real-time progress tracking
- ✅ Comprehensive error handling and retry logic
- ✅ Configurable export presets
- ✅ Validation and diagnostic tools

The setup process is designed to be comprehensive but straightforward. If you encounter any issues, use the troubleshooting guide and diagnostic tools provided.

For additional help, refer to the command-line tools and web interface for real-time validation and troubleshooting assistance.