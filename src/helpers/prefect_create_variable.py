text = """params {
    fastq = {{ fq_dir }}
    references = "/mnt/cephfs8_rw/nanopore2/service/reference/human/hg38/no_alt"
    reference_mmi_file = "/mnt/cephfs8_rw/nanopore2/service/reference/human/hg38/no_alt/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.mmi"
    out_dir = {{ bam_out_dir }}
    prefix = {{ prefix }}
    per_read_stats = true
    depth_coverage = true
    threads = {{ threads_per_alignment }}
}

process {
    withName: "pipeline:process_references:combine" {
        memory = 8.GB
    }

report {
    enabled = true
    file = {{ report_file }}
    overwrite = true
    }

trace {
    enabled = true
    file = {{ trace_file }}
    overwrite = true
    }

timeline {
    enabled = true
    file = {{ timeline_file }}
    overwrite = true
    }
}

workDir = {{ sample_work_dir }}"""

print(text.replace('\n', '\\n').replace('"', '\\"'))

"""
params {\n    fastq = {{ fq_dir }}\n    references = \"/mnt/cephfs8_rw/nanopore2/service/reference/human/hg38/no_alt\"\n    reference_mmi_file = \"/mnt/cephfs8_rw/nanopore2/service/reference/human/hg38/no_alt/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.mmi\"\n    out_dir = {{ bam_out_dir }}\n    prefix = {{ prefix }}\n    per_read_stats = true\n    depth_coverage = true\n    threads = {{ threads_per_alignment }}\n}\n\nprocess {\n    withName: \"pipeline:process_references:combine\" {\n        memory = 8.GB\n    }\n\nreport {\n    enabled = true\n    file = {{ report_file }}\n    overwrite = true\n    }\n\ntrace {\n    enabled = true\n    file = {{ trace_file }}\n    overwrite = true\n    }\n\ntimeline {\n    enabled = true\n    file = {{ timeline_file }}\n    overwrite = true\n    }\n}\n\nworkDir = {{ sample_work_dir }}
"""