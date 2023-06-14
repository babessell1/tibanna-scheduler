import json

def get_job_templates(inbucket, outbucket, inputs, inputs_idx, ebs_size, instance_types):

    job_templates = {
        "call_strling":  f'''{{
            "args": {{
                "app_name": "call-strling",
                "cwl_directory_local": "cwl/",
                "cwl_main_filename": "call_strling.cwl",
                "cwl_version": "v1",
                "input_files": {{
                    "crams": {{
                        "bucket_name": "{inbucket}",
                        "object_key": {inputs}
                    }},
                    "fasta": {{
                        "bucket_name": "niagads-bucket",
                        "object_key": "references/GRCh38_full_analysis_set_plus_decoy_hla.fa.gz",
                        "unzip": "gz"
                    }},
                    "cramsidx": {{
                        "bucket_name": "{inbucket}",
                        "object_key": {inputs_idx}
                    }},
                    "fastaidx": {{
                        "bucket_name": "{inbucket}",
                        "object_key": "references/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai.gz",
                        "unzip": "gz"
                    }}
                }},
                "output_S3_bucket": "{outbucket}",
                "output_target": {{
                    "out": "output/strling/"
            }},
                "secondary_output_target": {{}}
            }},
            "config": {{
                "ebs_size": {ebs_size},
                "instance_type": {json.dumps(instance_types)},
                "EBS_optimized": true,
                "password": "",
                "log_bucket": "niagads-out-bucket",
                "spot_instance": true,
                "key_name": "big-wgs-key"
            }}
        }}'''.replace("'", '"'),

        "your_template_here": f'''{{
        }}'''.replace("'", '"')

    }

    return job_templates