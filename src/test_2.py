from classes.sample import Sample
from pathlib import Path

from config import STAGE_DEPENDENCIES
from tasks import phased_cnv_extraction, star_alleles

sample = Sample(
                id='test',
                cnv=set([Path('/mnt/cephfs8_rw/nanopore2/service/code/github/neurology/cyp2d6/result/772015791501/cnv_calling_no_subflow/20250416_0758_P2S/')]),
                )

