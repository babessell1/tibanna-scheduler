import json

def get_job_templates(inbucket, outbucket, inputs, inputs_idx, ebs_size, instance_types):

    job_templates = {

        "index":  f'''{{
            "args": {{
                "app_name": "call-strling",
                "cwl_directory_local": "cwl/",
                "cwl_main_filename": "index.cwl",
                "cwl_version": "v1",
                "input_files": {{
                    "crams": {{
                        "bucket_name": "{inbucket}",
                        "object_key": {inputs}
                    }}
                }},
                "output_S3_bucket": "{inbucket}",
                "output_target": {{
                    "cramsidx": "output/index/"
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

        "call_strling":  f'''{{
            "args": {{
                "app_name": "call-melt",
                "cwl_directory_local": "cwl/",
                "cwl_main_filename": "call_melt.cwl",
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
                    "fastaidx": {{
                        "bucket_name": "{inbucket}",
                        "object_key": "references/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai.gz",
                        "unzip": "gz"
                    }},
		    "melt": {{
                        "bucket_name": "{inbucket}",
                        "object_key": "software/MELTv2.2.2.tar.gz,
                        "unzip": "gz"
                    }}
                }},
                "output_S3_bucket": "{outbucket}",
                "output_target": {{
                    "out": "output/melt/"
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

def get_output_target_key(job_template):
    print(job_template)
    job_config = json.loads(job_template)
    output_target = job_config['args']['output_target']
    if len(output_target) != 1:
        raise ValueError("Expected exactly one key in the output_target section.")
    output_key = next(iter(output_target))
    return output_key
