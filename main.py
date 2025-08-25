#!/usr/bin/env python3
"""
Humaein Case Study #1: Claim Resubmission Ingestion Pipeline
Healthcare data engineering pipeline for processing EMR claim data
"""

import json
import csv
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
import re
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ClaimProcessor:
    """Main pipeline class for processing EMR claim data"""
    
    def __init__(self):
        self.unified_claims = []
        self.metrics = {
            'total_processed': 0,
            'source_alpha_count': 0,
            'source_beta_count': 0,
            'resubmission_candidates': 0,
            'excluded_claims': 0,
            'malformed_records': 0
        }
        
        # Known retryable and non-retryable denial reasons
        self.retryable_reasons = {
            "missing modifier", "incorrect npi", "prior auth required"
        }
        self.non_retryable_reasons = {
            "authorization expired", "incorrect provider type"
        }
        
        # Current date for age calculation (as specified in requirements)
        self.current_date = datetime(2025, 7, 30)
    
    def normalize_denial_reason(self, reason: str) -> str:
        """Normalize denial reason text for consistent matching"""
        if not reason:
            return None
        return reason.lower().strip()
    
    def classify_ambiguous_denial(self, reason: str) -> bool:
        """
        Mock LLM classifier for ambiguous denial reasons
        In production, this would call an actual LLM API
        """
        if not reason:
            return False
            
        reason_lower = reason.lower().strip()
        
        # Hardcoded classification for demo purposes
        ambiguous_retryable = {
            "form incomplete": True,
            "incorrect procedure": True,  # Could be a simple coding error
            "not billable": False,  # Usually a fundamental issue
        }
        
        # Simple heuristic: if it mentions "incomplete", "missing", "wrong" it might be retryable
        heuristic_keywords = ["incomplete", "missing", "wrong", "error", "typo"]
        if any(keyword in reason_lower for keyword in heuristic_keywords):
            logger.info(f"Classified '{reason}' as retryable via heuristic")
            return True
            
        return ambiguous_retryable.get(reason_lower, False)
    
    def parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string in various formats"""
        if not date_str:
            return None
            
        try:
            # Handle ISO format
            if 'T' in date_str:
                return datetime.fromisoformat(date_str.replace('T', ' ').replace('Z', ''))
            # Handle simple date format
            return datetime.strptime(date_str, '%Y-%m-%d')
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return None
    
    def ingest_csv_source(self, file_path: str) -> List[Dict]:
        """Ingest and normalize CSV data from EMR Alpha"""
        try:
            logger.info(f"Processing CSV source: {file_path}")
            
            # Create sample data if file doesn't exist
            if not Path(file_path).exists():
                logger.info("Creating sample CSV data")
                sample_data = [
                    "claim_id,patient_id,procedure_code,denial_reason,submitted_at,status",
                    "A123,P001,99213,Missing modifier,2025-07-01,denied",
                    "A124,P002,99214,Incorrect NPI,2025-07-10,denied",
                    "A125,,99215,Authorization expired,2025-07-05,denied",
                    "A126,P003,99381,None,2025-07-15,approved",
                    "A127,P004,99401,Prior auth required,2025-07-20,denied"
                ]
                with open(file_path, 'w') as f:
                    f.write('\n'.join(sample_data))
            
            df = pd.read_csv(file_path)
            records = []
            
            for _, row in df.iterrows():
                try:
                    # Handle 'None' string as null
                    denial_reason = row.get('denial_reason')
                    if denial_reason == 'None' or pd.isna(denial_reason):
                        denial_reason = None
                    
                    # Handle empty patient_id
                    patient_id = row.get('patient_id')
                    if pd.isna(patient_id) or patient_id == '':
                        patient_id = None
                    
                    record = {
                        'claim_id': str(row['claim_id']),
                        'patient_id': patient_id,
                        'procedure_code': str(row['procedure_code']),
                        'denial_reason': denial_reason,
                        'status': str(row['status']).lower(),
                        'submitted_at': self.parse_date(str(row['submitted_at'])),
                        'source_system': 'alpha'
                    }
                    records.append(record)
                    self.metrics['source_alpha_count'] += 1
                    
                except Exception as e:
                    logger.error(f"Malformed record in CSV: {row.to_dict()}, Error: {e}")
                    self.metrics['malformed_records'] += 1
            
            return records
            
        except Exception as e:
            logger.error(f"Failed to process CSV source: {e}")
            return []
    
    def ingest_json_source(self, file_path: str) -> List[Dict]:
        """Ingest and normalize JSON data from EMR Beta"""
        try:
            logger.info(f"Processing JSON source: {file_path}")
            
            # Create sample data if file doesn't exist
            if not Path(file_path).exists():
                logger.info("Creating sample JSON data")
                sample_data = [
                    {
                        "id": "B987",
                        "member": "P010",
                        "code": "99213",
                        "error_msg": "Incorrect provider type",
                        "date": "2025-07-03T00:00:00",
                        "status": "denied"
                    },
                    {
                        "id": "B988",
                        "member": "P011",
                        "code": "99214",
                        "error_msg": "Missing modifier",
                        "date": "2025-07-09T00:00:00",
                        "status": "denied"
                    },
                    {
                        "id": "B989",
                        "member": "P012",
                        "code": "99215",
                        "error_msg": None,
                        "date": "2025-07-10T00:00:00",
                        "status": "approved"
                    },
                    {
                        "id": "B990",
                        "member": None,
                        "code": "99401",
                        "error_msg": "incorrect procedure",
                        "date": "2025-07-01T00:00:00",
                        "status": "denied"
                    }
                ]
                with open(file_path, 'w') as f:
                    json.dump(sample_data, f, indent=2)
            
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            records = []
            for item in data:
                try:
                    record = {
                        'claim_id': str(item['id']),
                        'patient_id': item.get('member'),  # Can be None
                        'procedure_code': str(item['code']),
                        'denial_reason': item.get('error_msg'),  # Can be None
                        'status': str(item['status']).lower(),
                        'submitted_at': self.parse_date(item['date']),
                        'source_system': 'beta'
                    }
                    records.append(record)
                    self.metrics['source_beta_count'] += 1
                    
                except Exception as e:
                    logger.error(f"Malformed record in JSON: {item}, Error: {e}")
                    self.metrics['malformed_records'] += 1
            
            return records
            
        except Exception as e:
            logger.error(f"Failed to process JSON source: {e}")
            return []
    
    def is_eligible_for_resubmission(self, claim: Dict) -> tuple[bool, str]:
        """
        Determine if a claim is eligible for resubmission based on business rules
        Returns (eligible, reason)
        """
        # Rule 1: Status must be denied
        if claim['status'] != 'denied':
            return False, f"Status is '{claim['status']}', not denied"
        
        # Rule 2: Patient ID must not be null
        if not claim['patient_id']:
            return False, "Patient ID is null"
        
        # Rule 3: Claim must be older than 7 days
        if not claim['submitted_at']:
            return False, "Submitted date is null"
        
        days_old = (self.current_date - claim['submitted_at']).days
        if days_old <= 7:
            return False, f"Claim is only {days_old} days old (need >7)"
        
        # Rule 4: Denial reason must be retryable
        denial_reason = self.normalize_denial_reason(claim['denial_reason'])
        
        if not denial_reason:
            # Null denial reason - classify as ambiguous
            if self.classify_ambiguous_denial(denial_reason):
                return True, "Null denial reason classified as retryable"
            else:
                return False, "Null denial reason classified as non-retryable"
        
        # Check known retryable reasons
        if denial_reason in self.retryable_reasons:
            return True, f"Known retryable reason: '{denial_reason}'"
        
        # Check known non-retryable reasons
        if denial_reason in self.non_retryable_reasons:
            return False, f"Known non-retryable reason: '{denial_reason}'"
        
        # Ambiguous - use classifier
        if self.classify_ambiguous_denial(denial_reason):
            return True, f"Ambiguous reason '{denial_reason}' classified as retryable"
        else:
            return False, f"Ambiguous reason '{denial_reason}' classified as non-retryable"
    
    def generate_recommended_changes(self, claim: Dict) -> str:
        """Generate recommended changes for resubmission"""
        denial_reason = claim['denial_reason']
        if not denial_reason:
            return "Review claim details and resubmit with corrections"
        
        reason_lower = denial_reason.lower()
        
        recommendations = {
            "missing modifier": "Add the required modifier to the procedure code",
            "incorrect npi": "Review and correct the NPI number",
            "prior auth required": "Obtain prior authorization before resubmission",
            "incorrect procedure": "Review and correct the procedure code",
            "form incomplete": "Complete all required fields and resubmit"
        }
        
        return recommendations.get(reason_lower, f"Review and correct: {denial_reason}")
    
    def process_pipeline(self, csv_path: str = "emr_alpha.csv", json_path: str = "emr_beta.json"):
        """Main pipeline execution"""
        logger.info("Starting claim resubmission pipeline")
        
        # Ingest from both sources
        csv_records = self.ingest_csv_source(csv_path)
        json_records = self.ingest_json_source(json_path)
        
        # Combine all records
        self.unified_claims = csv_records + json_records
        self.metrics['total_processed'] = len(self.unified_claims)
        
        logger.info(f"Processed {len(self.unified_claims)} total claims")
        
        # Determine resubmission eligibility
        resubmission_candidates = []
        excluded_claims = []
        
        for claim in self.unified_claims:
            eligible, reason = self.is_eligible_for_resubmission(claim)
            
            if eligible:
                candidate = {
                    'claim_id': claim['claim_id'],
                    'resubmission_reason': claim['denial_reason'] or 'Unknown',
                    'source_system': claim['source_system'],
                    'recommended_changes': self.generate_recommended_changes(claim)
                }
                resubmission_candidates.append(candidate)
                self.metrics['resubmission_candidates'] += 1
            else:
                excluded_claims.append({
                    'claim_id': claim['claim_id'],
                    'exclusion_reason': reason,
                    'source_system': claim['source_system']
                })
                self.metrics['excluded_claims'] += 1
        
        # Save output
        with open('resubmission_candidates.json', 'w') as f:
            json.dump(resubmission_candidates, f, indent=2)
        
        # Save exclusion log
        with open('excluded_claims.json', 'w') as f:
            json.dump(excluded_claims, f, indent=2)
        
        # Log metrics
        self.log_metrics()
        
        return resubmission_candidates
    
    def log_metrics(self):
        """Log pipeline execution metrics"""
        logger.info("=== PIPELINE METRICS ===")
        logger.info(f"Total claims processed: {self.metrics['total_processed']}")
        logger.info(f"Claims from Alpha EMR: {self.metrics['source_alpha_count']}")
        logger.info(f"Claims from Beta EMR: {self.metrics['source_beta_count']}")
        logger.info(f"Flagged for resubmission: {self.metrics['resubmission_candidates']}")
        logger.info(f"Excluded from resubmission: {self.metrics['excluded_claims']}")
        logger.info(f"Malformed records: {self.metrics['malformed_records']}")
        logger.info("========================")


def main():
    """Main execution function"""
    processor = ClaimProcessor()
    candidates = processor.process_pipeline()
    
    print(f"\nFound {len(candidates)} claims eligible for resubmission:")
    for candidate in candidates:
        print(f"- {candidate['claim_id']}: {candidate['resubmission_reason']}")
    
    print(f"\nResults saved to:")
    print("- resubmission_candidates.json")
    print("- excluded_claims.json")
    print("- pipeline.log")


if __name__ == "__main__":
    main()