# AmoCRM Export Features Guide

This guide covers the enhanced export features of the AmoCRM Data Exporter, including Google Sheets export with progress tracking, preset management, and advanced configuration options.

## Table of Contents

1. [Overview](#overview)
2. [Google Sheets Export](#google-sheets-export)
3. [Export Presets](#export-presets)
4. [Progress Tracking](#progress-tracking)
5. [Error Handling](#error-handling)
6. [Configuration Management](#configuration-management)
7. [Troubleshooting](#troubleshooting)

## Overview

The AmoCRM Data Exporter provides multiple ways to export your CRM data:

- **Excel Export**: Traditional Excel file export with all data
- **Google Sheets Export**: Direct export to Google Sheets with real-time progress tracking
- **Preset-based Export**: Customized exports using saved field configurations
- **Enhanced Progress Tracking**: Real-time monitoring of export operations

### Supported Entity Types

The following entity types are supported for export:

- **Deals** (Leads): Sales opportunities and deal information
- **Contacts**: Customer and prospect contact information
- **Companies**: Organization and company data
- **Users**: System users and team members
- **Pipelines**: Sales pipeline configurations

> **Note**: Events are excluded from export operations as per system requirements.

## Google Sheets Export

### Basic Google Sheets Export

1. **Navigate to the main dashboard**
2. **Set date filters** (optional):
   - Date From: Start date for filtering records
   - Date To: End date for filtering records
3. **Click "Export to Google Sheets"**
4. **Monitor progress** in the progress tracking interface

### Enhanced Google Sheets Export

The enhanced export provides:
- Real-time progress tracking
- Better error handling and recovery
- Batch processing optimization
- WebSocket-based status updates

To start an enhanced export:

1. **Go to Export Progress page** (`/progress`)
2. **Set date filters** if needed
3. **Click "Start Enhanced Export"**
4. **Monitor real-time progress** with detailed entity-level tracking

### Export Results

After successful export, you'll receive:
- Direct links to exported Google Sheets
- Summary statistics (records exported, duration)
- Entity-specific spreadsheet URLs

## Export Presets

Export presets allow you to save custom field selections and configurations for reuse across multiple exports.

### Creating Export Presets

1. **Navigate to Preset Management** (`/preset-management`)
2. **Click "Create Preset" tab**
3. **Fill in preset details**:
   - **Name**: Descriptive name for the preset
   - **Entity Type**: Select the entity type (deals, contacts, etc.)
   - **Description**: Optional description
4. **Select fields**:
   - Check the fields you want to include in exports
   - Fields are organized by type (standard, custom)
   - Preview data is shown for each field
5. **Click "Create Preset"**

### Managing Presets

#### Viewing Presets
- All presets are displayed in a grid layout
- Filter by entity type using the dropdown
- Each card shows preset name, entity type, field count, and dates

#### Editing Presets
1. **Click "Edit"** on any preset card
2. **Modify** name, description, or field selection
3. **Save changes**

#### Duplicating Presets
1. **Click "Duplicate"** on any preset card
2. **Enter new name** for the duplicate
3. **Preset is created** with same field selection

#### Deleting Presets
1. **Click "Delete"** on any preset card
2. **Confirm deletion** (this action cannot be undone)

### Using Presets for Export

1. **Go to Preset Management** → **"Export with Presets" tab**
2. **Select presets** for each entity type you want to export
3. **Set date filters** (optional)
4. **Click "Start Export"**
5. **View results** with direct spreadsheet links

## Progress Tracking

### Real-time Progress Monitoring

The progress tracking system provides:

- **Overall export progress** with percentage completion
- **Entity-level progress** showing individual entity status
- **Batch processing information** with current/total batches
- **Processing rates** (items per second)
- **Estimated completion times**
- **Error reporting** with suggested actions

### Progress Interface Features

#### Export Status Indicators
- **Pending**: Export queued but not started
- **Starting**: Export initialization in progress
- **In Progress**: Active data processing
- **Completed**: Export finished successfully
- **Failed**: Export encountered errors
- **Cancelled**: Export was manually cancelled

#### Real-time Updates
- **WebSocket connection** for instant updates
- **Connection status indicator** (connected/disconnected)
- **Automatic reconnection** if connection is lost
- **Batch progress notifications**

#### Export Management
- **Cancel running exports** with confirmation
- **View export history** and results
- **Direct links** to exported spreadsheets

### WebSocket Integration

The system uses WebSocket connections for real-time updates:

```javascript
// WebSocket connection is established automatically
// Updates are received in real-time without page refresh
// Connection status is displayed in the top-right corner
```

## Error Handling

### Comprehensive Error Recovery

The system includes robust error handling:

#### Configuration Errors
- **Missing spreadsheet IDs**: Clear setup instructions provided
- **Invalid credentials**: OAuth renewal guidance
- **Permission issues**: Specific permission requirements listed

#### API Errors
- **Rate limits**: Automatic exponential backoff retry
- **Network issues**: Intelligent retry with circuit breaker
- **Authentication failures**: Token refresh and re-authentication

#### Data Processing Errors
- **Large datasets**: Automatic batch size adjustment
- **Memory issues**: Streaming processing implementation
- **Format errors**: Graceful degradation with warnings

### Error Messages and Actions

When errors occur, the system provides:
- **User-friendly error messages**
- **Suggested corrective actions**
- **Detailed logging** for troubleshooting
- **Partial export recovery** when possible

### Retry Mechanisms

The system implements intelligent retry logic:

```
Retry Configuration:
- Max retries: 3 attempts
- Base delay: 1 second
- Max delay: 60 seconds
- Exponential backoff: 2x multiplier
- Jitter: Random delay variation
```

## Configuration Management

### Google Sheets Configuration

#### Required Configuration
1. **Google Sheets API credentials** (`credentials.json`)
2. **OAuth token** (`token.json`)
3. **Spreadsheet IDs** for each entity type

#### Configuration Validation
- **Automatic validation** on system startup
- **Manual validation** via Configuration Validation page
- **Detailed error reporting** with setup guidance

#### Setup Process
1. **Enable Google Sheets API** in Google Cloud Console
2. **Create service account** or OAuth credentials
3. **Download credentials file**
4. **Configure spreadsheet IDs** in environment variables
5. **Run validation** to confirm setup

### Environment Variables

Required environment variables:

```bash
# Google Sheets Configuration
GOOGLE_SHEETS_LEADS_ID=your_leads_spreadsheet_id
GOOGLE_SHEETS_CONTACTS_ID=your_contacts_spreadsheet_id
GOOGLE_SHEETS_COMPANIES_ID=your_companies_spreadsheet_id

# Optional: Custom batch sizes
EXPORT_BATCH_SIZE=1000
MAX_CONCURRENT_EXPORTS=2
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Google Sheets Export Fails

**Symptoms**: Export starts but fails with authentication error

**Solutions**:
- Check credentials file exists and is valid
- Verify OAuth token is not expired
- Ensure spreadsheet IDs are correct
- Confirm spreadsheet permissions allow editing

#### 2. Progress Tracking Not Working

**Symptoms**: Progress page shows "Connecting..." or no updates

**Solutions**:
- Check WebSocket connection (connection status indicator)
- Refresh the page to reconnect
- Verify firewall/proxy settings allow WebSocket connections
- Check browser console for JavaScript errors

#### 3. Preset Creation Fails

**Symptoms**: "Failed to create preset" error message

**Solutions**:
- Ensure preset name is unique for the entity type
- Select at least one field for export
- Check MongoDB connection is working
- Verify entity type is supported

#### 4. Large Dataset Export Timeouts

**Symptoms**: Export fails with timeout errors on large datasets

**Solutions**:
- Use date filters to reduce dataset size
- Enable batch processing optimization
- Increase timeout settings in configuration
- Monitor memory usage during export

#### 5. Custom Fields Not Appearing

**Symptoms**: Custom fields missing from field selection

**Solutions**:
- Ensure custom fields have data in AmoCRM
- Check field permissions in AmoCRM
- Refresh field cache in export settings
- Verify custom field types are supported

### Performance Optimization

#### For Large Datasets
- **Use date filters** to limit data scope
- **Enable batch processing** with optimal batch sizes
- **Monitor memory usage** during exports
- **Use preset-based exports** to limit fields

#### For Better Reliability
- **Ensure stable internet connection**
- **Configure appropriate retry settings**
- **Monitor Google Sheets API quotas**
- **Use incremental exports** when possible

### Getting Help

#### Log Files
- Check application logs for detailed error information
- Logs are organized by component (sheets, progress, presets)
- Error logs include stack traces for debugging

#### Support Information
- Include export ID when reporting issues
- Provide relevant log entries
- Describe steps to reproduce the problem
- Include configuration details (without sensitive data)

#### Diagnostic Tools
- **Configuration Validation**: Test Google Sheets setup
- **Progress Tracking**: Monitor export status
- **Health Check**: Verify system components
- **Connection Testing**: Validate API connections

## Best Practices

### Export Strategy
1. **Start with small datasets** to test configuration
2. **Use presets** for consistent field selection
3. **Monitor progress** during large exports
4. **Set appropriate date filters** to manage data volume
5. **Test configuration** before production exports

### Data Management
1. **Regular backups** of export configurations
2. **Preset organization** with descriptive names
3. **Field selection optimization** for performance
4. **Date range planning** for incremental exports
5. **Error monitoring** and response procedures

### System Maintenance
1. **Regular credential renewal** for Google Sheets
2. **Monitor API quotas** and usage
3. **Update configuration** as needed
4. **Clean up old exports** and logs
5. **Performance monitoring** and optimization

---

For additional support or questions, please refer to the system logs or contact your system administrator.