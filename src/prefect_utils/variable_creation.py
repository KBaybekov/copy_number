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
}

workDir = {{ sample_work_dir }}"""

print(text.replace('\n', '\\n').replace('"', '\\"'))