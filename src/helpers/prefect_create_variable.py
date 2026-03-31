text = """params {
    cnv = true
    sv = true
    bam = "{{ bam_dir }}"
    ref = "/mnt/cephfs8_rw/nanopore2/service/reference/human/hg38/no_alt"
    bam_min_coverage = 20
    out_dir = "{{ cnv_out_dir }}"
    threads = {{ threads_per_cnv_calling }}
    phased = true
    override_basecaller_cfg = "{{ basecalling_model }}"
}

report {
    enabled = true
    file = "{{ report_file }}"
    overwrite = true
    }

trace {
    enabled = true
    file = "{{ trace_file }}"
    overwrite = true
    }

timeline {
    enabled = true
    file = "{{ timeline_file }}"
    overwrite = true
    }

workDir = "{{ sample_work_dir }}"
"""

print(text.replace('\n', '\\n').replace('"', '\\"'))

"""
params {\n    cnv = true\n    sv = true\n    bam = \"{{ bam_dir }}\"\n    ref = \"/mnt/cephfs8_rw/nanopore2/service/reference/human/hg38/no_alt\"\n    bam_min_coverage = 20\n    out_dir = \"{{ cnv_out_dir }}\"\n    threads = {{ threads_per_cnv_calling }}\n    phased = true\n    override_basecaller_cfg = \"{{ basecalling_model }}\"\n}\n\nreport {\n    enabled = true\n    file = \"{{ report_file }}\"\n    overwrite = true\n    }\n\ntrace {\n    enabled = true\n    file = \"{{ trace_file }}\"\n    overwrite = true\n    }\n\ntimeline {\n    enabled = true\n    file = \"{{ timeline_file }}\"\n    overwrite = true\n    }\n\nworkDir = \"{{ sample_work_dir }}\"\n
"""

"/mnt/cephfs8_ro/nanopore/DNA/RONC/770131741501/20250217_1356_P2S-02571-B_PAW75802_92ee5798/fastq_pass"