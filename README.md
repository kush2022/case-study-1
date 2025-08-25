# Humaein Case Study #1: Claim Resubmission Ingestion Pipeline

## Overview

This project implements a healthcare data engineering pipeline for processing Electronic Medical Record (EMR) claim data, with a focus on identifying claims eligible for resubmission. The pipeline ingests claim data from two EMR systems (Alpha and Beta), applies business rules to determine resubmission eligibility, and outputs results and metrics for further review.

## Features

- Ingests and normalizes claim data from:
  - CSV files (EMR Alpha)
  - JSON files (EMR Beta)
- Applies configurable business rules to determine claim resubmission eligibility:
  - Status must be "denied"
  - Patient ID must be present
  - Claim must be older than 7 days
  - Denial reason must be retryable (with support for ambiguous reasons via a mock classifier)
- Generates recommended changes for resubmission based on denial reasons
- Outputs:
  - `resubmission_candidates.json`: Claims eligible for resubmission with recommendations
  - `excluded_claims.json`: Claims excluded from resubmission with reasons
  - `pipeline.log`: Detailed execution log and metrics

## Requirements

- Python 3.7+
- Packages:
  - pandas

Install dependencies with:

```bash
pip install pandas
```

## Usage

1. **Prepare Input Files**

   By default, the script expects:
   - `emr_alpha.csv` (CSV data from EMR Alpha)
   - `emr_beta.json` (JSON data from EMR Beta)

   If these files do not exist, the script will generate sample data for demonstration.

2. **Run the Pipeline**

   From the `case_study_1` directory, execute:

   ```bash
   python main.py
   ```

3. **Review Output**

   After execution, the following files will be generated/updated:
   - `resubmission_candidates.json`: List of claims eligible for resubmission
   - `excluded_claims.json`: List of claims excluded from resubmission with reasons
   - `pipeline.log`: Log file with detailed processing information and metrics

## Pipeline Workflow

1. **Ingestion**
   - Reads and normalizes data from both CSV and JSON sources.
   - Handles missing or malformed records gracefully, logging any issues.

2. **Eligibility Determination**
   - Applies business rules to each claim to determine resubmission eligibility.
   - Uses a mock classifier for ambiguous denial reasons (can be replaced with an actual ML/LLM model).

3. **Output Generation**
   - Writes eligible claims and recommendations to `resubmission_candidates.json`.
   - Writes excluded claims and reasons to `excluded_claims.json`.
   - Logs metrics and processing details to `pipeline.log`.

## Example Output

After running the script, you will see console output similar to:

```
Found 3 claims eligible for resubmission:
- A123: Missing modifier
- A124: Incorrect NPI
- A127: Prior auth required

Results saved to:
- resubmission_candidates.json
- excluded_claims.json
- pipeline.log
```

## Customization

- **Business Rules:**  
  The rules for resubmission eligibility and denial reason classification can be extended or modified in the `ClaimProcessor` class.
- **Input Files:**  
  You can provide your own `emr_alpha.csv` and `emr_beta.json` files with the expected schema.

## File Structure

```
case_study_1/
├── main.py
├── emr_alpha.csv
├── emr_beta.json
├── resubmission_candidates.json
├── excluded_claims.json
├── pipeline.log
└── README.md
```

## License

This project is provided for educational and demonstration purposes.