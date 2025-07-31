# AmoCRM Export Troubleshooting Guide

This guide provides solutions to common issues encountered when using the AmoCRM Data Exporter, with a focus on Google Sheets export functionality.

## Table of Contents

1. [Quick Diagnostics](#quick-diagnostics)
2. [Google Sheets Export Issues](#google-sheets-export-issues)
3. [Progress Tracking Problems](#progress-tracking-problems)
4. [Preset Management Issues](#preset-management-issues)
5. [Performance Problems](#performance-problems)
6. [Configuration Issues](#configuration-issues)
7. [Network and Connectivity](#network-and-connectivity)
8. [Data Processing Errors](#data-processing-errors)
9. [System Resources](#system-resources)
10. [Advanced Troubleshooting](#advanced-troubleshooting)

## Quick Diagnostics

### Health Check

Before troubleshooting specific issues, check the system health:

1. **Visit the health endpoint**: `http://your-server/health`
2. **Check service status**:
   - MongoDB: Should show "connected"
   - RabbitMQ: Should show "connected"
   - Overall status: Should be "healthy"

### Configuration Validation

1. **Go to Configuration Validation page**: `/config-validation`
2. **Run validation tests**
3. **Review any errors or warnings**
4. **Follow provided setup instructions**

### Log Review

Check recent logs for errors:
1. **Go to main dashboard**
2. **Scroll to logs section**
3. **Filter by error level**
4. **Look for recent error messages**

## Google Sheets Export Issues

### Issue: "Google Sheets configuration is invalid"

**Symptoms**:
- Export button is disabled
- Error message about invalid configuration
- Configuration validation fails

**Solutions**:

1. **Check credentials file**:
   ```bash
   # Verify credentials.json exists and is valid JSON
   ls -la credentials.json
   cat credentials.json | python -m json.tool
   ```

2. **Verify environment variables**:
   ```bash
   # Check required spreadsheet IDs are set
   echo $GOOGLE_SHEETS_LEADS_ID
   echo $GOOGLE_SHEETS_CONTACTS_ID
   echo $GOOGLE_SHEETS_COMPANIES_ID
   ```

3. **Test OAuth token**:
   - Delete `token.json` if it exists
   - Restart the application
   - Complete OAuth flow when prompted

4. **Verify spreadsheet permissions**:
   - Open each spreadsheet URL in browser
   - Ensure you have edit permissions
   - Check sharing settings

### Issue: "Authentication failed" during export

**Symptoms**:
- Export starts but fails quickly
- Error mentions authentication or credentials
- Token-related error messages

**Solutions**:

1. **Refresh OAuth token**:
   ```bash
   # Remove existing token
   rm token.json
   # Restart application to trigger re-authentication
   ```

2. **Check Google Cloud Console**:
   - Verify API is enabled
   - Check quota limits
   - Review credentials configuration

3. **Verify service account** (if using service account):
   - Check service account has necessary permissions
   - Verify key file is not expired
   - Ensure service account is shared on spreadsheets

### Issue: "Spreadsheet not found" or permission errors

**Symptoms**:
- Error about spreadsheet ID not found
- Permission denied errors
- Cannot access spreadsheet

**Solutions**:

1. **Verify spreadsheet IDs**:
   ```bash
   # Extract ID from spreadsheet URL
   # https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit
   # Use only the SPREADSHEET_ID part
   ```

2. **Check spreadsheet sharing**:
   - Open spreadsheet in browser
   - Click "Share" button
   - Add service account email (if using service account)
   - Grant "Editor" permissions

3. **Test spreadsheet access**:
   - Try opening spreadsheet manually
   - Verify you can edit cells
   - Check if spreadsheet is in correct Google account

### Issue: Export completes but data is missing or incorrect

**Symptoms**:
- Export shows success but spreadsheet is empty
- Some entities missing from export
- Data formatting issues

**Solutions**:

1. **Check date filters**:
   - Verify date range includes expected data
   - Remove date filters to test full export
   - Check date format (YYYY-MM-DD)

2. **Verify data in AmoCRM**:
   - Confirm data exists in AmoCRM
   - Check entity permissions
   - Verify custom fields have values

3. **Review field selection**:
   - Check if using presets with limited fields
   - Verify all required fields are selected
   - Test with default field selection

## Progress Tracking Problems

### Issue: Progress page shows "Connecting..." indefinitely

**Symptoms**:
- WebSocket connection fails
- No real-time updates
- Connection status shows "Disconnected"

**Solutions**:

1. **Check WebSocket support**:
   - Verify browser supports WebSockets
   - Test with different browser
   - Check browser console for errors

2. **Network configuration**:
   - Verify firewall allows WebSocket connections
   - Check proxy settings
   - Test direct connection without proxy

3. **Server configuration**:
   - Ensure WebSocket endpoint is accessible
   - Check server logs for WebSocket errors
   - Verify port configuration

### Issue: Progress updates are delayed or inconsistent

**Symptoms**:
- Progress updates arrive late
- Missing progress notifications
- Inconsistent status updates

**Solutions**:

1. **Check connection stability**:
   - Monitor network connection
   - Look for connection drops in logs
   - Test with stable network connection

2. **Server performance**:
   - Check server resource usage
   - Monitor memory and CPU usage
   - Verify database performance

3. **Browser performance**:
   - Close unnecessary browser tabs
   - Clear browser cache
   - Test with different browser

## Preset Management Issues

### Issue: "Failed to create preset" error

**Symptoms**:
- Preset creation fails with error message
- Form validation errors
- Database connection issues

**Solutions**:

1. **Check form validation**:
   - Ensure preset name is unique
   - Select at least one field
   - Verify entity type is selected

2. **Database connectivity**:
   - Check MongoDB connection
   - Verify database permissions
   - Test database write operations

3. **Field loading issues**:
   - Refresh entity fields
   - Check AmoCRM API connection
   - Verify entity data exists

### Issue: Presets not loading or displaying incorrectly

**Symptoms**:
- Preset list is empty
- Loading spinner never stops
- Preset data appears corrupted

**Solutions**:

1. **Database query issues**:
   - Check MongoDB connection
   - Verify collection exists
   - Test database read operations

2. **Data corruption**:
   - Check preset data format
   - Verify JSON structure
   - Look for invalid characters

3. **Cache issues**:
   - Clear browser cache
   - Refresh the page
   - Test in incognito mode

## Performance Problems

### Issue: Export is very slow or times out

**Symptoms**:
- Export takes much longer than expected
- Timeout errors during export
- High memory usage

**Solutions**:

1. **Optimize batch size**:
   ```bash
   # Adjust batch size in configuration
   export EXPORT_BATCH_SIZE=500  # Reduce from default 1000
   ```

2. **Use date filters**:
   - Limit export to specific date ranges
   - Export data incrementally
   - Focus on recent data first

3. **Reduce field selection**:
   - Use presets to limit fields
   - Exclude unnecessary custom fields
   - Focus on essential data only

4. **System resources**:
   - Monitor memory usage
   - Check CPU utilization
   - Ensure adequate disk space

### Issue: High memory usage during export

**Symptoms**:
- System becomes slow during export
- Out of memory errors
- Application crashes

**Solutions**:

1. **Enable streaming processing**:
   - Configure batch processing
   - Reduce batch sizes
   - Enable memory-efficient mode

2. **System optimization**:
   - Increase available memory
   - Close unnecessary applications
   - Monitor memory usage

3. **Data optimization**:
   - Use smaller date ranges
   - Limit field selection
   - Process entities separately

## Configuration Issues

### Issue: Environment variables not loading

**Symptoms**:
- Configuration validation fails
- Missing spreadsheet IDs
- Default values being used

**Solutions**:

1. **Check .env file**:
   ```bash
   # Verify .env file exists and has correct format
   cat .env
   # Ensure no spaces around = signs
   # Verify no quotes unless needed
   ```

2. **Environment loading**:
   ```bash
   # Check if variables are loaded
   printenv | grep GOOGLE_SHEETS
   # Restart application after changes
   ```

3. **File permissions**:
   ```bash
   # Ensure .env file is readable
   chmod 644 .env
   # Check file ownership
   ls -la .env
   ```

### Issue: Database connection failures

**Symptoms**:
- MongoDB connection errors
- Data not saving or loading
- Health check shows database disconnected

**Solutions**:

1. **Check MongoDB service**:
   ```bash
   # Verify MongoDB is running
   systemctl status mongod
   # Or for Docker
   docker ps | grep mongo
   ```

2. **Connection string**:
   ```bash
   # Verify MongoDB connection string
   echo $MONGODB_URL
   # Test connection manually
   mongo $MONGODB_URL
   ```

3. **Network connectivity**:
   - Check firewall rules
   - Verify port accessibility
   - Test network connectivity

## Network and Connectivity

### Issue: API rate limiting errors

**Symptoms**:
- "Rate limit exceeded" errors
- Export fails with quota messages
- Slow API responses

**Solutions**:

1. **Implement backoff strategy**:
   - System automatically retries with exponential backoff
   - Monitor retry attempts in logs
   - Adjust retry configuration if needed

2. **Optimize API usage**:
   - Use batch requests where possible
   - Implement request caching
   - Reduce concurrent requests

3. **Check API quotas**:
   - Review Google Sheets API quotas
   - Monitor usage in Google Cloud Console
   - Consider quota increase if needed

### Issue: Network timeouts during export

**Symptoms**:
- Connection timeout errors
- Intermittent network failures
- Export fails randomly

**Solutions**:

1. **Network stability**:
   - Test network connection stability
   - Check for intermittent connectivity issues
   - Use wired connection if possible

2. **Timeout configuration**:
   ```bash
   # Increase timeout values
   export API_TIMEOUT=60  # seconds
   export NETWORK_TIMEOUT=30  # seconds
   ```

3. **Retry configuration**:
   - Increase retry attempts
   - Adjust retry delays
   - Enable circuit breaker pattern

## Data Processing Errors

### Issue: Custom fields not processing correctly

**Symptoms**:
- Custom fields appear as raw JSON
- Field names are technical IDs
- Custom field values are missing

**Solutions**:

1. **Field metadata refresh**:
   - Clear field cache
   - Reload entity fields
   - Verify custom field configuration

2. **Data format issues**:
   - Check custom field types in AmoCRM
   - Verify field value formats
   - Test with simple custom fields first

3. **Processing configuration**:
   - Enable graceful degradation
   - Check custom field processor settings
   - Review field mapping configuration

### Issue: Date and time formatting problems

**Symptoms**:
- Dates appear as numbers
- Time zones are incorrect
- Date formats are inconsistent

**Solutions**:

1. **Format configuration**:
   - Check date format settings
   - Verify timezone configuration
   - Test with known date values

2. **Data source issues**:
   - Verify date formats in AmoCRM
   - Check timestamp conversion
   - Test with different date fields

3. **Spreadsheet formatting**:
   - Check Google Sheets date formatting
   - Verify cell format settings
   - Test manual date entry

## System Resources

### Issue: Disk space errors

**Symptoms**:
- "No space left on device" errors
- Export files not being created
- Temporary file issues

**Solutions**:

1. **Check disk space**:
   ```bash
   # Check available disk space
   df -h
   # Check specific directories
   du -sh /path/to/exports
   ```

2. **Clean up files**:
   ```bash
   # Remove old export files
   find exports/ -name "*.xlsx" -mtime +30 -delete
   # Clean temporary files
   rm -rf /tmp/export_*
   ```

3. **Configure cleanup**:
   - Enable automatic file cleanup
   - Set retention policies
   - Monitor disk usage

### Issue: Memory leaks during long exports

**Symptoms**:
- Memory usage increases over time
- System becomes progressively slower
- Application eventually crashes

**Solutions**:

1. **Monitor memory usage**:
   ```bash
   # Monitor memory usage
   top -p $(pgrep -f "python.*exporter")
   # Check memory leaks
   ps aux | grep exporter
   ```

2. **Optimize processing**:
   - Enable streaming processing
   - Reduce batch sizes
   - Implement memory cleanup

3. **System configuration**:
   - Increase available memory
   - Configure swap space
   - Set memory limits

## Advanced Troubleshooting

### Debug Mode

Enable debug logging for detailed troubleshooting:

```bash
# Set debug log level
export LOG_LEVEL=DEBUG
# Enable detailed logging
export DETAILED_LOGGING=true
# Restart application
```

### Database Debugging

Check database operations:

```bash
# Connect to MongoDB
mongo your_database_name
# Check collections
show collections
# Query export data
db.export_progress.find().limit(5)
db.export_presets.find().limit(5)
```

### Network Debugging

Test network connectivity:

```bash
# Test Google Sheets API
curl -I "https://sheets.googleapis.com/v4/spreadsheets"
# Test WebSocket connection
wscat -c ws://localhost:8000/ws/progress
# Check DNS resolution
nslookup sheets.googleapis.com
```

### Performance Profiling

Profile application performance:

```bash
# Enable profiling
export ENABLE_PROFILING=true
# Monitor performance metrics
# Check processing times in logs
grep "processing time" logs/application.log
```

### Log Analysis

Analyze logs for patterns:

```bash
# Search for errors
grep -i error logs/*.log
# Check export patterns
grep "export.*completed" logs/*.log
# Monitor memory usage
grep "memory" logs/*.log
```

## Getting Additional Help

### Information to Collect

When seeking help, collect the following information:

1. **System Information**:
   - Operating system and version
   - Python version
   - Application version
   - Available memory and disk space

2. **Configuration Details**:
   - Environment variables (without sensitive data)
   - Configuration file contents
   - Google Sheets setup details

3. **Error Information**:
   - Complete error messages
   - Stack traces from logs
   - Steps to reproduce the issue
   - Export IDs for failed exports

4. **Performance Data**:
   - Export duration and data size
   - Memory usage during export
   - Network connectivity details
   - API response times

### Log Collection

Collect relevant logs:

```bash
# Application logs
tail -n 100 logs/application.log > debug_logs.txt
# Error logs
grep -i error logs/*.log > error_logs.txt
# Export-specific logs
grep "export_id_here" logs/*.log > export_logs.txt
```

### Support Channels

- Check system documentation
- Review configuration guides
- Consult application logs
- Contact system administrator

---

This troubleshooting guide covers the most common issues. For specific problems not covered here, enable debug logging and review the detailed logs for additional information.