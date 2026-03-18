from prefect.variables import Variable

cfg_template = Variable.get('nxf_cfg_alignment_v1')

print(cfg_template)