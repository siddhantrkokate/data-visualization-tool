# DVT Platform - Data Validation Tool

A web-based data validation and quality assurance platform powered by AI. Upload CSV or Excel files, configure validation rules using AI, and generate intelligent validation scripts automatically.

---

## Features

- **User Authentication** - Email-based OTP verification with session management
- **Project Management** - Create, organize, and manage data validation projects
- **AI-Powered Column Scoring** - Automatically score column relevance based on project category
- **AI Validation Rules** - Generate custom validation prompts for each column
- **Dynamic Script Generation** - AI creates Python validation scripts automatically
- **Real-Time Processing** - Track validation progress with live status updates
- **Data Export** - Download validated data, filtered results, or high-accuracy rows
- **Quality Metrics** - Get Efficiency Scores for every row (0-100)
- **Color-Coded Output** - XLSX files with visual indicators (green/amber/red)

---

## Quick Start

### Prerequisites

- Python 3.8+
- MySQL Server
- Gmail account with App Password
- Krutrim AI API key (from cloud.olakrutrim.com)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/siddhantrkokate/data-visualization-tool.git
cd data-visualization-tool
```

2. Install dependencies:
```bash
pip install flask mysql-connector-python pandas openpyxl requests
```

3. Run the application:
```bash
python app.py
```

The app will launch an interactive setup wizard to configure:
- MySQL database credentials
- Gmail SMTP credentials (for OTP emails)
- Krutrim AI API key

4. Open your browser and navigate to:
```
http://localhost:5000
```

---

## Usage Workflow

1. **Sign In** - Enter your email to receive OTP verification code
2. **Create Project** - Name your project, select category, upload CSV/Excel file
3. **Score Columns** - AI analyzes and scores each column's relevance (0-100)
4. **Configure Rules** - AI generates validation rules for each column
5. **Run Validation** - AI generates Python script and validates all rows
6. **Export Results** - Download validated data with Efficiency Scores

---

## Project Structure

```
data-visualization-tool/
├── app.py                       # Main Flask application
├── README.md                    # This file
├── dvt_platform_backup.sql      # Database schema
├── config.json                  # Configuration (auto-generated)
├── files/                       # Uploaded and processed files
│   ├── [uuid].csv              # Original CSV uploads
│   ├── [uuid].xlsx             # Generated validation output
├── runner/                      # AI-generated scripts
│   ├── [uuid].py               # Validation script
│   └── [uuid]_status.json      # Processing status
└── templates/                   # HTML templates
    ├── auth.html               # Login/OTP page
    ├── home.html               # Project list
    ├── create.html             # Project creation
    └── project.html            # Project workspace
```

---

## API Endpoints

### Authentication
- POST `/send-otp` - Send OTP to email
- POST `/verify-otp` - Verify OTP and login
- POST `/resend-otp` - Resend OTP
- GET `/check-session` - Check login status
- GET `/logout` - Logout user

### Projects
- GET `/` - List all projects (home)
- GET `/create` - Create project form
- POST `/create-project` - Create new project
- GET `/project/<id>` - View project details
- POST `/delete-project/<id>` - Delete project

### AI Features
- POST `/project/<id>/score-columns` - Score column relevance
- POST `/project/<id>/generate-prompts` - Generate validation rules
- GET `/project/<id>/get-script/<job_id>` - View generated script

### Validation & Data
- POST `/project/<id>/start-processing` - Start validation
- GET `/project/<id>/processing-status/<job_id>` - Check validation progress
- POST `/project/<id>/stop-processing/<job_id>` - Stop validation
- GET `/project/<id>/full-data` - Get all validated rows
- POST `/project/<id>/download-filtered` - Download filtered data
- GET `/project/<id>/download-high-accuracy` - Download high-accuracy rows

---

## Technology Stack

- **Backend**: Python, Flask
- **Database**: MySQL
- **Data Processing**: Pandas, OpenPyXL
- **AI**: Krutrim AI API
- **Email**: Gmail SMTP
- **Frontend**: HTML5, JavaScript, CSS3

---

## Database Schema

### users
- `user_id` (INT, Primary Key)
- `email` (VARCHAR, Unique)
- `created_at` (TIMESTAMP)

### otp_veri
- `id` (INT, Primary Key)
- `email` (VARCHAR)
- `otp` (VARCHAR)
- `created_at` (TIMESTAMP)

### projects
- `project_id` (INT, Primary Key)
- `email` (VARCHAR, Foreign Key)
- `project_name` (VARCHAR)
- `project_category` (VARCHAR)
- `project_date` (DATE)
- `project_time` (TIME)
- `file_name` (VARCHAR)
- `created_at` (TIMESTAMP)

---

## AI Validation Process

### Phase 1: Column Scoring
AI analyzes column names in context of project category and assigns scores 0-100:
- 75-100: Core, critical columns
- 50-74: Important but secondary
- 25-49: Marginally relevant
- 0-24: Irrelevant

### Phase 2: Rule Generation
AI generates validation prompts for each column:
Example: "Email column - must be non-empty, must be valid email format, max 100 characters"

### Phase 3: Script Generation
AI creates a complete Python validation script with:
- Per-column validation functions
- Automatic Efficiency Score calculation
- Color-coded Excel output

### Phase 4: Validation Execution
Script runs with:
- Real-time progress tracking
- Cell-level failure highlighting (red)
- Row-level accuracy scoring (green/amber/red)

---

## Efficiency Score

The Efficiency Score (0-100) for each row is calculated as:

```
Efficiency_Score = (number_of_columns_passed / total_columns) * 100
```

Color coding:
- GREEN: >= 75 (High Accuracy)
- AMBER: 50-74 (Medium Accuracy)
- RED: < 50 (Low Accuracy)

Example output:
```
Name         | Email               | Phone      | Efficiency_Score
John Doe     | john@example.com    | 555-1234   | 100.00 (GREEN)
Jane Smith   | jane_invalid        | 555-5678   | 66.67 (AMBER)
Bob Jones    | bob@             | invalid    | 33.33 (RED)
```

---

## Security Features

- Email OTP authentication with 5-minute expiry
- Session-based access control
- SQL injection prevention (parameterized queries)
- Secure file uploads with UUID naming
- Input validation and sanitization
- CSRF protection in forms
- Error handling without exposing sensitive data
- Database credentials stored in config.json (not in code)

---

## Configuration

After setup, edit `config.json` to adjust settings:

```json
{
  "db_host": "localhost",
  "db_user": "root",
  "db_pass": "your_password",
  "sender_email": "your_email@gmail.com",
  "email_pass": "your_app_password",
  "krutrim_api_key": "your_api_key"
}
```

---

## File Upload

Supported formats:
- CSV (.csv)
- Excel (.xlsx)

Maximum file size: Limited by Flask configuration (default 16MB)

---

## Troubleshooting

### Email not sending?
- Check Gmail App Password (not regular password)
- Verify sender email in config.json
- Ensure Gmail SMTP is accessible from your network

### Database connection failed?
- Verify MySQL is running
- Check credentials in config.json
- Ensure dvt_platform database exists

### AI API errors?
- Verify Krutrim API key is valid
- Check internet connection
- Confirm API endpoint is reachable

### Validation script fails?
- Check input file format (CSV or XLSX)
- Ensure file is not corrupted
- Review validation rules for conflicts

---

## Development

### Adding New Routes
1. Define endpoint in app.py
2. Add @login_required decorator if needed
3. Create corresponding HTML template if UI needed

### Modifying AI Prompts
AI prompts are defined in:
- Line 435: Column scoring
- Line 510: Validation rule generation
- Line 576: Script generation

### Database Changes
Edit `dvt_platform_backup.sql` and reimport with:
```bash
mysql -h localhost -u root -p dvt_platform < dvt_platform_backup.sql
```

---

## License

This project is provided as-is for data validation purposes.

---

## Support

For issues or feature requests, please create a GitHub issue in this repository.

---

## Author

Created by Siddhant Kokate

Last updated: April 2026
