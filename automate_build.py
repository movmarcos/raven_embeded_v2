#%%
import sys, argparse
from libs.utils import get_project_root

project_root = get_project_root()
sys.path.insert(0, f'{project_root}/libs')
from libs.build_raven import BuildRaven

#### Get arguments ####

# arguments will be passed in via the powershell script
parser = argparse.ArgumentParser(description='Raven Embedded deploy automation')
parser.add_argument('-db', '--SnowflakeTargetDB', help='(STRING) Snowflake database name', required=True)
parser.add_argument('-envn', '--EnvironmentNumber', help='(STRING) Snowflake database name', required=True)
parser.add_argument('-app', '--AppName', help='(STRING) App name', required=True)
parser.add_argument('-metadata', '--FlagMetadata', help='(BOOLEAN) Flag: Upload metadata files and staging', required=True)
parser.add_argument('-build', '--FlagBuild', help='(BOOLEAN) Flag: Execute build', required=True)
parser.add_argument('-grant', '--FlagGrant', help='(BOOLEAN) Flag: Grant permissions to EXEC role', required=True)

args = vars(parser.parse_args())
print(args)
database_name = args['SnowflakeTargetDB']
app_name = args['AppName']
flag_metadata = args['FlagMetadata']
flag_build = args['FlagBuild']
flag_grant = args['FlagGrant']
env_number = args['EnvironmentNumber'] if args['EnvironmentNumber'] else 0

build = BuildRaven(database_name, app_name, flag_metadata, flag_build, flag_grant,env_number)
result_dict = build.build_raven()
