# Clay Integration Guide

**Step-by-step setup for WebsiteScraper v2 in Clay**

## 🎯 **Overview**

This creates a **2-column setup** in Clay:
1. **Column A**: Creates scrape job → Returns `job_id` instantly  
2. **Column B**: Polls for results → Returns full HTML when done

## 📋 **Prerequisites**

- ✅ API deployed to Vercel
- ✅ Worker running on your Lenovo  
- ✅ Auth token: `WSCRAPER_PROD_6q2dVNYxJ4`

## 🔧 **Column A: Create Scrape Job**

### **Basic Setup**
1. Create new column → **HTTP Enrichment**
2. Name: "Website Scraper Job"

### **Configuration**
**URL**: `https://your-app.vercel.app/api/scrape`  
**Method**: `POST`

**Headers**:
```
Authorization: Bearer WSCRAPER_PROD_6q2dVNYxJ4
Content-Type: application/json
```

### **Request Body**
```json
{
  "url": "{{company.domain}}",
  "render_js": false,
  "max_wait_ms": 12000,
  "block_assets": ["image","font","media","stylesheet","analytics"],
  "priority": "normal",
  "idempotency_key": "row_{{row.id}}"
}
```

### **Extract Response**
Map `$.job_id` to a new column called **"Job ID"**

**Expected Response:**
```json
{
  "status": "queued",
  "job_id": "scr_abc123def456",
  "eta_ms": 60000
}
```

## 🔄 **Column B: Get Results**

### **Basic Setup**
1. Create new column → **HTTP Enrichment**  
2. Name: "Website Content"

### **Configuration**
**URL**: `https://your-app.vercel.app/api/result?job_id={{Job ID}}`  
**Method**: `GET`

**Headers**:
```
Authorization: Bearer WSCRAPER_PROD_6q2dVNYxJ4
```

### **Run Condition** (IMPORTANT!)
```javascript
{{ this.status != "done" && this.status != "failed" }}
```

This makes Clay keep polling until the job completes.

### **Final Response Structure**
When done, you'll get:
```json
{
  "status": "done",
  "job_id": "scr_abc123def456",
  "result": {
    "url": "https://example.com",
    "final_url": "https://example.com/",
    "status_code": 200,
    "title": "Example Domain",
    "headers": {"content-type": "text/html"},
    "html_body": "<!DOCTYPE html><html>...FULL HTML...</html>",
    "pdf_urls": ["https://example.com/docs.pdf"],
    "meta": {
      "fetch_ms": 8234,
      "retries": 0,
      "proxy_used": "45.196.33.57:6038",
      "render_js": false,
      "bytes_rx": 52130,
      "worker_id": "worker_lenovo_123"
    }
  }
}
```

## 📊 **Create Data Columns**

From the result, create these columns:

### **Column C: HTML Content**
- **Source**: `$.result.html_body`
- **Type**: Text
- **Note**: Contains full HTML regardless of size

### **Column D: Final URL**  
- **Source**: `$.result.final_url`
- **Type**: URL

### **Column E: Page Title**
- **Source**: `$.result.title`  
- **Type**: Text

### **Column F: Status Code**
- **Source**: `$.result.status_code`
- **Type**: Number

### **Column G: PDF Links**
- **Source**: `$.result.pdf_urls`
- **Type**: Array

### **Column H: Fetch Time**
- **Source**: `$.result.meta.fetch_ms`
- **Type**: Number (milliseconds)

## ⚙️ **Advanced Options**

### **For JavaScript-Heavy Sites**
Change request body to:
```json
{
  "url": "{{company.domain}}",
  "render_js": true,
  "max_wait_ms": 20000,
  "block_assets": ["image","font","media"],
  "priority": "normal"
}
```

### **For Priority Processing**
```json
{
  "url": "{{company.domain}}",
  "priority": "high",
  "max_wait_ms": 15000
}
```

### **For Minimal Blocking** (more accurate)
```json
{
  "url": "{{company.domain}}",
  "block_assets": ["image","media"],
  "max_wait_ms": 15000
}
```

## 🚨 **Error Handling**

### **Common Error Responses**
```json
{
  "status": "failed",
  "job_id": "scr_abc123def456",
  "error": {
    "code": "BLOCKED",
    "message": "403 after 3 attempts"
  }
}
```

**Error codes:**
- `INVALID_URL` - Bad URL format
- `NETWORK` - Connection issues  
- `TIMEOUT` - Page took too long
- `BLOCKED` - Blocked by website (403/429)
- `INTERNAL` - Server error

### **Handle Errors in Clay**
Create a condition column:
```javascript
{{ this.status == "done" ? "Success" : this.error.code }}
```

## 📏 **Rate Limiting**

**API Limits:**
- 100 requests/minute per IP
- No limit on job results polling

**Worker Capacity:**
- 20 concurrent jobs per worker
- ~300 jobs per hour sustained

## 🧪 **Testing Your Setup**

### **Test URLs**
Start with these reliable test sites:

```
https://example.com
https://httpbin.org/html  
https://github.com/microsoft/playwright
https://stackoverflow.com
```

### **Expected Timing**
- **Job creation**: <2 seconds
- **Simple sites**: 8-15 seconds total
- **Complex sites**: 15-30 seconds total
- **JS rendering**: +5-10 seconds

### **Troubleshooting**

**"Job not found" error:**
- Check if worker is running: `npm run health`
- Verify Job ID was captured correctly

**"Timeout" in Clay:**
- Make sure run condition is set correctly
- Check Clay's timeout settings (increase if needed)

**"All jobs failing":**
- Check worker logs: `tail -f worker.log`
- Test proxy connectivity
- Restart worker if needed

## 📈 **Performance Tips**

### **For Large Batches (1000+ URLs)**
1. **Start small**: Test 10 URLs first
2. **Monitor worker**: Keep `npm run health` open in another tab
3. **Batch processing**: Process 500 rows at a time
4. **Check progress**: Job creation should be instant, results come in waves

### **Expected Performance**
- **100 URLs**: ~10-15 minutes
- **500 URLs**: ~45-60 minutes  
- **1000 URLs**: ~90-120 minutes
- **5000 URLs**: ~8-15 hours (overnight run)

### **Memory Management**
Worker auto-restarts if memory exceeds 8GB, so large batches can run unattended.

## 🎉 **You're Ready!**

Your Clay setup should now:
- ✅ Create scrape jobs instantly
- ✅ Poll for results automatically  
- ✅ Return full HTML content
- ✅ Handle errors gracefully
- ✅ Scale to thousands of URLs

**Start with 10-20 test URLs to verify everything works, then scale up!**