from pathlib import Path

p = Path('/home/kbajbekov@pak-cspmz.ru/work/neurology/cyp2d6/copy_number/src/file_format_handlers/').resolve()
print([x for x in p.parents])