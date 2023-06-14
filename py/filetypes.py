import sys


# file extention: filetype, index extension
filetypes = {
    "cram": ["cram", "crai"],
    "fa": ["fasta", "fai"],
    "fasta": ["fasta", "fai"],
    "fna": ["fasta", "fai"],
    "bam": ["bam", "bai"]
}

def get_filetype(filenames):
    ftype, idx_ext = filetypes[filenames[0].split(".")[-1]]
    if len(filenames)>1:
        for file in filenames[1:]:
            next_ftype, _ = filetypes[file.split(".")[-1]]
            if ftype != next_ftype:
                raise ValueError(f"""
                    Filetype {ftype} does not match {next_ftype}!\n
                    Input filetypes MUST be the same, auxilary files like reference fastas can be specified in the job description file.
                """)
            sys.exit(1)
    
    return ftype, idx_ext