from waflib import Logs
from waflib.extras.preparation import PreparationContext
from waflib.extras.build_status import BuildStatus

def options(optCtx):
    optCtx.recurse('env')
    optCtx.recurse('src')
    optCtx.recurse('test')

def prepare(prepCtx):
    prepCtx.recurse('env')
    prepCtx.recurse('src')

def configure(confCtx):
    confCtx.recurse('env')
    confCtx.recurse('src')
    confCtx.recurse('test')
    
def build(buildCtx):
    status = BuildStatus.init(buildCtx.path.abspath())
    if status.isSuccess() and not(buildCtx.is_install):
	Logs.pprint('NORMAL', 'Build already complete                   :', sep='')
	Logs.pprint('GREEN', 'skipping')
	return
    buildCtx.recurse('src')
    status.setSuccess()
    buildCtx.recurse('test')
