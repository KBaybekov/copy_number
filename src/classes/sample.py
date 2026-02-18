from __future__ import annotations
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from modules.logger import get_logger
from file_format_handlers.tsv_handler import write_sample_data
from config import ONT_FOLDER, RES_FOLDER, RAW_DATA_THRESHOLD, BASECALLED_DATA_THRESHOLD
from inspect import stack
from threading import RLock


logger = get_logger(__name__)


def get_files_sizes_in_Gb(
                          subdir:Path,
                          extension:str
                         ) -> float:
    full_size = sum([
                     f.stat().st_size
                     for f in subdir.iterdir()
                     if f.name.endswith(extension)
                    ])
    return (full_size / 1024 ** 3)

@dataclass(slots=True)
class Sample:
    id:str #from_dict
    db_id:str #from_dict
    group:str #from_dict
    subgroup:str #from_dict
    panno:str = field(default="") #from_dict
    copy_number:Optional[float] = field(default=None)
    copy_number_s1:Optional[float] = field(default=None)
    copy_number_s2:Optional[float] = field(default=None)
    res_folder:Optional[Path] = field(default=Path()) #gather_ont_metadata
    work_folder:Optional[Path] = field(default=Path()) #gather_ont_metadata
    src_folder:Optional[Path] = field(default=Path()) #gather_ont_metadata
    f5_pass_size_Gb:float = field(default=0) #gather_ont_metadata
    f5_fail_size_Gb:float = field(default=0) #gather_ont_metadata
    p5_pass_size_Gb:float = field(default=0) #gather_ont_metadata
    p5_fail_size_Gb:float = field(default=0) #gather_ont_metadata
    fq_pass_size_Gb:float = field(default=0) #gather_ont_metadata
    fq_fail_size_Gb:float = field(default=0) #gather_ont_metadata
    # data size of unbasecalled f5_pass+p5_pass
    raw_data_size_Gb:float = field(default=0) #gather_ont_metadata
    enough_raw_data:bool = field(default=False) #gather_ont_metadata
    enough_basecalled_data:bool = field(default=False) #gather_ont_metadata
    basecalled_batches:Set[Path] = field(default_factory=set) #gather_ont_metadata, ...
    unbasecalled_batches:Set[Path] = field(default_factory=set) #gather_ont_metadata, ...
    depth_val:Optional[float] = field(default=None)
    cyp2d6_mapq:Optional[float] = field(default=None)
    depth_file:Optional[Path] = field(default=Path())
    fq_folders:Set[Path] = field(default_factory=set) #gather_ont_metadata
    bams:Set[Path] = field(default_factory=set)
    merged_bams:Set[Path] = field(default_factory=set)
    bam:Optional[Path] = field(default=Path())
    bam_qc:bool = field(default=False)
    sv:Optional[Path] = field(default=Path())
    cnv:Optional[Path] = field(default=Path())
    fail_reason:str = field(default="")
    errors:Dict[str, str] =  field(default_factory=dict) # {stage:error}
    processed_tasks:List[str] = field(default_factory=list)
    task_statuses:Dict[str, str] =  field(default_factory=dict) # {stage:status}
    stage_statuses:Dict[str, str] =  field(default_factory=dict) # Статус для каждого этапа, включающего в себя n однотипных задач
    status:str = field(default="")
    success:bool = field(default=False)
    finished:bool = field(default=False)
    _lock:RLock = field(default_factory=RLock, repr=False, compare=False)
#!!! дбавить processed_tasks в from_dict; обновить to_dict; отработать новый run_universal_flow
    @staticmethod
    def from_dict(
                  data:Dict[str, str],
                 ) -> 'Sample':
        def get_float_val(dict_val:Any) -> Optional[float]:
            def is_float(value):
                try:
                    float(value)
                    return True
                except ValueError:
                    return False
                
            match dict_val:
                case str():
                    if is_float(dict_val):
                        return float(dict_val)
                    else:
                        return None
                case int():
                     return float(dict_val)
                case _:
                    return None
                
        def get_path_val(dict_val:Any) -> Optional[Path]:
            match dict_val:
                case str():
                    if dict_val in ['','.']:
                        return None
                    else:
                        return Path(dict_val).resolve()
                case _:
                    return None
                
        def get_bool_val(dict_val:Any) -> bool:
            match dict_val:
                case 'True':
                    return True
                case _:
                    return False
                
        def get_set_of_paths(dict_val:Any) -> Set[Path]:
            match dict_val:
                case str(x) if len(x) > 0:
                    restored_paths = [get_path_val(p) for p in x.split('; ')]
                    return set([p for p in restored_paths if isinstance(p, Path)])
                case _:
                    return set()
        
        def split_str_to_dict(dict_val:Any) -> Dict[str, str]:
            match dict_val:
                case str(x) if len(x) > 0:
                    elements = dict_val.split('; ')
                    return {
                            element.split(': ')[0]: element.split(': ')[1]
                            for element in elements
                           }
                case _:
                    return {}

        def get_list_of_strings(dict_val:Any) -> List[str]:
            match dict_val:
                case str(x) if len(x) > 0:
                    return dict_val.split('; ')
                case _:
                    return []

        sample = Sample(
                        id=data['id'],
                        db_id=data['db_id'],
                        panno=data['panno'],
                        group=data['group'],
                        subgroup=data['subgroup'],
                        copy_number=get_float_val(data.get('copy_number')),
                        copy_number_s1=get_float_val(data.get('copy_number_s1')),
                        copy_number_s2=get_float_val(data.get('copy_number_s2')),
                        res_folder=get_path_val(data.get('res_folder')),
                        work_folder=get_path_val(data.get('work_folder')),
                        src_folder= get_path_val(data.get('src_folder')),
                        f5_pass_size_Gb=float(data['f5_pass_size_Gb']),
                        f5_fail_size_Gb=float(data['f5_fail_size_Gb']),
                        p5_pass_size_Gb=float(data['p5_pass_size_Gb']),
                        p5_fail_size_Gb=float(data['p5_fail_size_Gb']),
                        fq_pass_size_Gb=float(data['fq_pass_size_Gb']),
                        fq_fail_size_Gb=float(data['fq_fail_size_Gb']),
                        raw_data_size_Gb=float(data['raw_data_size_Gb']),
                        enough_raw_data=get_bool_val(data.get('enough_raw_data')),
                        enough_basecalled_data=get_bool_val(data.get('enough_basecalled_data')),
                        basecalled_batches=get_set_of_paths(data.get('basecalled_batches')),
                        unbasecalled_batches=get_set_of_paths(data.get('unbasecalled_batches')),
                        depth_val=get_float_val(data.get('depth_val')),
                        cyp2d6_mapq=get_float_val(data.get('cyp2d6_mapq')),
                        depth_file=get_path_val(data.get('depth_file')),
                        fq_folders=get_set_of_paths(data.get('fq_folders')),
                        bams=get_set_of_paths(data.get('bams')),
                        merged_bams=get_set_of_paths(data.get('merged_bams')),
                        bam=get_path_val(data.get('bam')),
                        bam_qc=get_bool_val(data.get('bam_qc')),
                        sv=get_path_val(data.get('sv')),
                        cnv=get_path_val(data.get('cnv')),
                        errors=split_str_to_dict(data.get('errors')),
                        processed_tasks=get_list_of_strings(data.get('processed_tasks')),
                        task_statuses=split_str_to_dict(data.get('task_statuses')),
                        stage_statuses=split_str_to_dict(data.get('stage_statuses')),
                        status=data['status'],
                        success=get_bool_val(data.get('success')),
                        finished=get_bool_val(data.get('finished'))                       
                       )
        #sample.gather_ont_metadata()
        return sample
    
    @staticmethod
    def from_dict_init(
                  data:Dict[str, str],
                 ) -> 'Sample':
        sample = Sample(
                        id=data['sample'],
                        db_id=data['DB_ID'],
                        panno=data['Panno'],
                        group=data['group'],
                        subgroup=data['subgroup']
                       )
        sample.gather_ont_metadata()
        return sample
    
    def to_dict(self) -> Dict[str, Any]:
        with self._lock: # Защищаем чтение всех полей сразу
            # Не используем asdict(self), так как он споткнется об RLock
            # Просто берем все поля, кроме служебных
            obj_dict = {
                k: getattr(self, k) 
                for k in self.__slots__ 
                if k != '_lock'
            }

        for k,v in obj_dict.items():
            match v:
                case None:
                    obj_dict[k] = ""
                case Path():
                    if v not in [Path(''), Path('.')]:
                        obj_dict[k] = v.as_posix()
                    else:
                        obj_dict[k] = ""
                case float():
                    obj_dict[k] = round(v, 2)
                case set():
                    paths = [x for x in v if isinstance(x, Path) and x.name != '.' and x.name != '']
                    obj_dict[k] = "; ".join(x.as_posix() for x in paths) if paths else ''
                case list():
                    obj_dict[k] = '; '.join([str(x) for x in v])
                case dict():
                    obj_dict[k] = '; '.join([f"{x}: {y}" for x,y in v.items()])
                case _:
                    pass
        return obj_dict
        
    def gather_ont_metadata(
                            self
                           ) -> None:
        stage_name = "metadata_gathering"
        def get_subdir_metadata(subdir:Path) -> None:
            match subdir.name:
                # fast5
                case 'fast5_fail'|'fast5_skip':
                    self.f5_fail_size_Gb += get_files_sizes_in_Gb(subdir,
                                                               '.fast5')
                case 'fast5_pass':
                    data_size = get_files_sizes_in_Gb(subdir,
                                                               '.fast5')
                    self.f5_pass_size_Gb += data_size
                    self.raw_data_size_Gb += data_size
                # pod5
                case 'pod5_fail'|'pod5_skip':
                    self.p5_fail_size_Gb+= get_files_sizes_in_Gb(subdir,
                                                               '.pod5')
                case 'pod5'|'pod5_pass':
                    data_size = get_files_sizes_in_Gb(subdir,
                                                               '.pod5')
                    self.p5_pass_size_Gb+= data_size
                    self.raw_data_size_Gb+= data_size
                # fastq
                case 'fastq_fail'|'fastq_skip':
                    self.fq_fail_size_Gb+= get_files_sizes_in_Gb(subdir,
                                                               '.fastq.gz')
                case 'fastq_pass':
                    self.fq_pass_size_Gb+= get_files_sizes_in_Gb(subdir,
                                                               '.fastq.gz')
                    self.fq_folders.add(subdir)
        # get sample folder
        self.src_folder = (ONT_FOLDER / self.group / self.subgroup / self.id).resolve()
        # check its existence
        if self.src_folder.is_dir():
            # get results folder
            self.res_folder = (RES_FOLDER / self.id).resolve()
            self.work_folder = self.res_folder / 'work'
            for d in [self.res_folder, self.work_folder]:
                d.mkdir(parents=True, exist_ok=True)
            # gather batch folders
            self.unbasecalled_batches = set([x for x in self.src_folder.iterdir() if x.is_dir()])
            # gather stats over batch files
            for batch_dir in self.unbasecalled_batches:
                batch_subdirs = [x for x in batch_dir.iterdir() if x.is_dir()]
                for subdir in batch_subdirs:
                    # if it contains any fastq dirs, add batch to basecalled
                    if all(['fastq' in subdir.name,
                            any(subdir.iterdir())]):
                        self.basecalled_batches.add(batch_dir)
                    get_subdir_metadata(subdir)
            # remove basecalled batch from unbasecalled 
            for batch_dir in self.basecalled_batches:
                        self.unbasecalled_batches.discard(batch_dir)
            # Define if sample can be processed by data thresholds
            self.enough_raw_data = (self.raw_data_size_Gb > RAW_DATA_THRESHOLD)
            self.enough_basecalled_data = (self.fq_pass_size_Gb > BASECALLED_DATA_THRESHOLD)
            # Filter out small samples
            if not any([
                        self.enough_raw_data,
                        self.enough_basecalled_data
                       ]):
                self.fail()
                self.log_sample_data(
                                     stage_name=stage_name,
                                     sample_ok=False,
                                     fail_reason="НЕДОСТАТОЧНО ИСХОДНЫХ ДАННЫХ",
                                     critical_error=True
                                    )
                return None
        else:
            self.log_sample_data(
                                 stage_name=stage_name,
                                 sample_ok=False,
                                 fail_reason=f"Директория не существует:{self.src_folder}",
                                 critical_error=True
                                )
            return None
        # logging data & updating status
        self.log_sample_data(
                             stage_name=stage_name,
                             sample_ok=True
                            )
        return None
        
    def fail(
             self,
             stage_name:str = "Unmarked_stage",
             reason:str = ""
            ) -> None:
        # никаких дедлоков, т.к. _lock - ReentryLock
        with self._lock:
            self.success = False
            self.finished = True
            self.log_sample_data(stage_name, False, critical_error=True, fail_reason=reason)
            return None

    def log_sample_data(
                        self,
                        stage_name:str,
                        sample_ok:bool,
                        critical_error:bool=False,
                        fail_reason:str = ""
                       ) -> None:
        
        # Получаем стек вызовов
        func_stack = stack()
        # [1] — это предыдущий фрейм (откуда вызвали)
        caller_frame = func_stack[1]
        filename = Path(caller_frame.filename).name
        func_name = caller_frame.function
        # Проверяем, вызван ли метод из `fail`
        if func_name == "fail":
            # Если да — поднимаемся ещё на уровень выше
            caller_frame = func_stack[2]
            filename = Path(caller_frame.filename).name
            func_name = caller_frame.function
        line_no = caller_frame.lineno
        func_trace = f"{func_name}@{filename}:{line_no}"

        with self._lock:
            self.status = stage_name
            self.fail_reason = fail_reason

            status_str = "FAIL"
            reason_in_msg = f", reason: {fail_reason}"
            log_severity = 30 # Warning
            
            match sample_ok, critical_error:
                # everything fine
                case True, False:
                    log_severity = 10 # Debug
                    reason_in_msg = ""
                    status_str = "OK"
                # something is broken
                case False, False:
                    self.errors[stage_name] = fail_reason
                # sample is fucked up
                case False, True:
                    self.errors[stage_name] = fail_reason
                    log_severity = 40 # Error
                    self.success = False
                    self.finished = True
            self.task_statuses[stage_name] = status_str
            msg = f"{status_str}: sample {self.id}, stage: {stage_name}{reason_in_msg}. Trace: {func_trace}"
            logger.log(msg=msg, level=log_severity)

            logger.debug(f"DATA_WRITING: sample {self.id}")
            write_sample_data(self.to_dict())
            return None
