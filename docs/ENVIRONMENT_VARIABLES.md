# Environment Variables Configuration Guide

This guide provides comprehensive information on configuring dbt-magics using environment variables, including examples for Snowflake and Athena setups.

## Overview

dbt-magics supports flexible configuration through environment variables, making it easier to:
- Work with multiple dbt projects
- Use different profiles.yml files
- Manage sensitive credentials securely
- Support dbt's native `env_var()` function syntax

## Supported Environment Variables

### Core Configuration Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `MAGICS_PROJECT_FOLDER` | Path to your dbt project directory (global fallback) | None | Yes* |
| `MAGICS_PROFILES_PATH` | Path to your profiles.yml file (global fallback) | `~/.dbt/profiles.yml` | No |
| `SNOWFLAKE_PROJECT_FOLDER` | Path to Snowflake-specific dbt project directory | Falls back to `MAGICS_PROJECT_FOLDER` | No |
| `SNOWFLAKE_PROFILES_PATH` | Path to Snowflake-specific profiles.yml file | Falls back to `MAGICS_PROFILES_PATH` | No |
| `ATHENA_PROJECT_FOLDER` | Path to Athena-specific dbt project directory | Falls back to `MAGICS_PROJECT_FOLDER` | No |
| `ATHENA_PROFILES_PATH` | Path to Athena-specific profiles.yml file | Falls back to `MAGICS_PROFILES_PATH` | No |
| `BIGQUERY_PROJECT_FOLDER` | Path to BigQuery-specific dbt project directory | Falls back to `MAGICS_PROJECT_FOLDER` | No |
| `BIGQUERY_PROFILES_PATH` | Path to BigQuery-specific profiles.yml file | Falls back to `MAGICS_PROFILES_PATH` | No |

*Required unless specified in profiles.yml under `project_folder` key.

**Note**: Adapter-specific variables (e.g., `SNOWFLAKE_PROJECT_FOLDER`) take precedence over generic variables (e.g., `MAGICS_PROJECT_FOLDER`). This allows you to use multiple adapters in the same notebook without conflicts.

### Custom Variables in profiles.yml

Any environment variable referenced in your profiles.yml using dbt's `env_var()` function will be automatically resolved.

## Configuration Methods

You can set environment variables using two methods:

### Method 1: IPython Magic Commands

```python
import os

# Set base paths
HOME = os.path.expanduser("~")
%env MAGICS_PROJECT_FOLDER={HOME}/projects/my-dbt-project
%env MAGICS_PROFILES_PATH={HOME}/.dbt/profiles.yml

# Set custom variables for your profiles.yml
%env SNOWFLAKE_ACCOUNT=my-account
%env SNOWFLAKE_USER=my-user
```

### Method 2: Python os.environ

```python
import os

# Set base paths
os.environ["MAGICS_PROJECT_FOLDER"] = os.path.expanduser("~/projects/my-dbt-project")
os.environ["MAGICS_PROFILES_PATH"] = os.path.expanduser("~/.dbt/profiles.yml")

# Set custom variables for your profiles.yml
os.environ["SNOWFLAKE_ACCOUNT"] = "my-account"
os.environ["SNOWFLAKE_USER"] = "my-user"
```

## Configuration Examples

### Snowflake Configuration

#### Notebook Configuration Cell

```python
import os
import pandas as pd 
import numpy as np
from datetime import datetime

# Configure DBT-MAGICS environment variables for dbt project 
# OPTION 1: Using IPython magic command 
HOME = os.path.expanduser("~")
%env MAGICS_PROJECT_FOLDER={HOME}/projects/data-aws/dbt_snowflake_dwh
%env MAGICS_PROFILES_PATH={HOME}/projects/data-aws/dbt_snowflake_dwh/profiles.yml
%env OKTA_USR=hans.dampf
%env SNOWFLAKE_USR=HANSDAMPF

# OPTION 2: Using python system libraries
os.environ["MAGICS_PROJECT_FOLDER"] = os.path.expanduser("~/projects/data-aws/dbt_snowflake_dwh")
os.environ["MAGICS_PROFILES_PATH"] = os.path.expanduser("~/projects/data-aws/dbt_snowflake_dwh/profiles.yml")
os.environ["OKTA_USR"] = 'stephan.feller'
os.environ["SNOWFLAKE_USR"] = 'STEPHANFELLER'

# Load the Snowflake magic
%reload_ext dbt_magics.snowflakeMagics
```

#### Corresponding profiles.yml

```yaml
snowflake_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USR', 'default_user') }}"
      authenticator: externalbrowser
      role: "{{ env_var('SNOWFLAKE_ROLE', 'ANALYST') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE', 'ANALYTICS') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA', 'PUBLIC') }}"
      threads: 4
      keepalives_idle: 240
      search_path: "{{ env_var('SNOWFLAKE_SEARCH_PATH', 'PUBLIC') }}"
      project_folder: "{{ env_var('MAGICS_PROJECT_FOLDER') }}"
      
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT_PROD') }}"
      user: "{{ env_var('SNOWFLAKE_USR') }}"
      authenticator: externalbrowser
      role: "{{ env_var('SNOWFLAKE_ROLE_PROD', 'PROD_ROLE') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE_PROD', 'PROD_DB') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE_PROD', 'PROD_WH') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA_PROD', 'PUBLIC') }}"
      threads: 8
      project_folder: "{{ env_var('MAGICS_PROJECT_FOLDER') }}"
```

### Athena Configuration

#### Notebook Configuration Cell

```python
import os
import pandas as pd 
import numpy as np
from datetime import datetime

# Configure DBT-MAGICS environment variables for dbt 
# OPTION 1: Using IPython magic command 
HOME = os.path.expanduser("~")
%env MAGICS_PROJECT_FOLDER={HOME}/projects/data-aws/dbt_athena_dwh
%env MAGICS_PROFILES_PATH={HOME}/.dbt/profiles.yml
%env AWS_REGION=us-east-1
%env ATHENA_S3_STAGING_DIR=s3://my-athena-results-bucket/

# OPTION 2: Using python system libraries
os.environ["MAGICS_PROJECT_FOLDER"] = os.path.expanduser("~/projects/data-aws/dbt_athena_dwh")
os.environ["MAGICS_PROFILES_PATH"] = os.path.expanduser("~/.dbt/profiles.yml")
os.environ["AWS_REGION"] = "us-east-1"
os.environ["ATHENA_S3_STAGING_DIR"] = "s3://my-athena-results-bucket/"

# Load the Athena magics
%reload_ext dbt_magics.athenaMagics
```

#### Corresponding profiles.yml

```yaml
athena_project:
  target: dev
  outputs:
    dev:
      type: athena
      s3_staging_dir: "{{ env_var('ATHENA_S3_STAGING_DIR') }}"
      region_name: "{{ env_var('AWS_REGION', 'us-east-1') }}"
      schema: "{{ env_var('ATHENA_SCHEMA', 'default') }}"
      database: "{{ env_var('ATHENA_DATABASE', 'default') }}"
      work_group: "{{ env_var('ATHENA_WORKGROUP', 'primary') }}"
      threads: 4
      aws_profile_name: "{{ env_var('AWS_PROFILE', 'default') }}"
      project_folder: "{{ env_var('MAGICS_PROJECT_FOLDER') }}"
      
    prod:
      type: athena
      s3_staging_dir: "{{ env_var('ATHENA_S3_STAGING_DIR_PROD') }}"
      region_name: "{{ env_var('AWS_REGION') }}"
      schema: "{{ env_var('ATHENA_SCHEMA_PROD', 'prod') }}"
      database: "{{ env_var('ATHENA_DATABASE_PROD', 'prod_db') }}"
      work_group: "{{ env_var('ATHENA_WORKGROUP_PROD', 'production') }}"
      threads: 8
      aws_profile_name: "{{ env_var('AWS_PROFILE_PROD', 'production') }}"
      project_folder: "{{ env_var('MAGICS_PROJECT_FOLDER') }}"
```

### Using Multiple Adapters in the Same Notebook

When comparing datasets during migrations or working with multiple data sources, you can use adapter-specific environment variables to avoid conflicts:

```python
import os
import pandas as pd 
import numpy as np
from datetime import datetime

# Configure Snowflake-specific variables
os.environ["SNOWFLAKE_PROJECT_FOLDER"] = os.path.expanduser("~/projects/data-aws/dbt_snowflake_dwh")
os.environ["SNOWFLAKE_PROFILES_PATH"] = os.path.expanduser("~/projects/data-aws/dbt_snowflake_dwh/profiles.yml")
os.environ["OKTA_USR"] = 'stephan.feller'
os.environ["SNOWFLAKE_USR"] = 'STEPHANFELLER'

# Configure Athena-specific variables
os.environ["ATHENA_PROJECT_FOLDER"] = os.path.expanduser("~/projects/data-aws/dbt_athena_dwh")
os.environ["ATHENA_PROFILES_PATH"] = os.path.expanduser("~/.dbt/profiles.yml")
os.environ["AWS_REGION"] = "us-east-1"
os.environ["ATHENA_S3_STAGING_DIR"] = "s3://my-athena-results-bucket/"

# Load both magics
%reload_ext dbt_magics.snowflakeMagics
%reload_ext dbt_magics.athenaMagics

# Now you can use both in the same notebook
%%snowflake
SELECT COUNT(*) as snowflake_count FROM {{ ref('my_model') }}

%%athena
SELECT COUNT(*) as athena_count FROM {{ ref('my_model') }}
```

**Priority Order for Configuration:**
1. **Adapter-specific env vars** (e.g., `SNOWFLAKE_PROJECT_FOLDER`, `ATHENA_PROFILES_PATH`)
2. **Generic env vars** (e.g., `MAGICS_PROJECT_FOLDER`, `MAGICS_PROFILES_PATH`)
3. **profiles.yml settings** (e.g., `project_folder` key)
4. **Default locations** (e.g., `~/.dbt/profiles.yml`)

## Configuration Fallbacks

dbt-magics follows this priority order for configuration:

### 1. Adapter-Specific Environment Variables (Highest Priority)

- `SNOWFLAKE_PROJECT_FOLDER` / `ATHENA_PROJECT_FOLDER` / `BIGQUERY_PROJECT_FOLDER`
- `SNOWFLAKE_PROFILES_PATH` / `ATHENA_PROFILES_PATH` / `BIGQUERY_PROFILES_PATH`

### 2. Generic Environment Variables

- `MAGICS_PROJECT_FOLDER`: Overrides any project_folder setting
- `MAGICS_PROFILES_PATH`: Overrides default profiles.yml location

### 3. profiles.yml Configuration

- `project_folder`: Used if no environment variables are set
- Standard dbt profile settings

### 4. Default Values (Lowest Priority)

- Profiles location: `~/.dbt/profiles.yml`
- No default for project folder (must be explicitly set)

## dbt env_var() Function Support

dbt-magics fully supports dbt's `env_var()` function with the following syntax:

```yaml
# With default value
key: "{{ env_var('VARIABLE_NAME', 'default_value') }}"

# Without default value (empty string if not found)
key: "{{ env_var('VARIABLE_NAME') }}"

# Using in complex values
connection_string: "host={{ env_var('DB_HOST') }};port={{ env_var('DB_PORT', '5432') }}"
```

### Supported Features

- ✅ Single and double quotes
- ✅ Default values
- ✅ Nested substitution
- ✅ Multiple env_var calls in one value
- ✅ Recursive processing of YAML structures

## Best Practices

### 1. Security

```python
# ❌ Don't hardcode sensitive values
os.environ["PASSWORD"] = "my-secret-password"

# ✅ Load from secure sources
import keyring
os.environ["SNOWFLAKE_PASSWORD"] = keyring.get_password("snowflake", "username")

# ✅ Use authentication methods that don't require passwords
# (e.g., externalbrowser for Snowflake, AWS IAM for Athena)
```

### 2. Environment Management

```python
# ✅ Use different profiles for different environments
%env MAGICS_PROFILES_PATH={HOME}/.dbt/dev_profiles.yml    # Development
%env MAGICS_PROFILES_PATH={HOME}/.dbt/prod_profiles.yml   # Production

# ✅ Use environment-specific variables
%env SNOWFLAKE_WAREHOUSE=DEV_WH     # Development
%env SNOWFLAKE_WAREHOUSE=PROD_WH    # Production
```

### 3. Project Organization

```python
# ✅ Organize projects clearly
os.environ["MAGICS_PROJECT_FOLDER"] = os.path.expanduser("~/dbt-projects/analytics")
os.environ["MAGICS_PROJECT_FOLDER"] = os.path.expanduser("~/dbt-projects/ml-pipeline")

# ✅ Use consistent naming conventions
%env ANALYTICS_DB_USER=analyst_user
%env ML_DB_USER=ml_user
```

## Troubleshooting

### Common Issues

#### 1. Project folder not found

```
Error: Path to the project is not set
```

**Solution**: Set `MAGICS_PROJECT_FOLDER` environment variable:

```python
os.environ["MAGICS_PROJECT_FOLDER"] = "/path/to/your/dbt/project"
```

#### 2. profiles.yml not found

```
Error: [Errno 2] No such file or directory: '~/.dbt/profiles.yml'
```

**Solution**: Set `MAGICS_PROFILES_PATH` to the correct location:

```python
os.environ["MAGICS_PROFILES_PATH"] = "/path/to/your/profiles.yml"
```

#### 3. Environment variable not resolved

```
Error: Environment variable 'SNOWFLAKE_ACCOUNT' not found
```

**Solution**: Ensure the variable is set before loading the magic:

```python
os.environ["SNOWFLAKE_ACCOUNT"] = "your-account-name"
%reload_ext dbt_magics.snowflakeMagics
```

### Debugging Tips

1. **Check current environment variables**:

```python
import os
print("MAGICS_PROJECT_FOLDER:", os.environ.get('MAGICS_PROJECT_FOLDER'))
print("MAGICS_PROFILES_PATH:", os.environ.get('MAGICS_PROFILES_PATH'))
```

2. **Verify profile loading**:

The magic will print the profiles.yml path being used when loading.

3. **Test env_var function directly**:

```python
from dbt_magics.dbt_helper import dbtHelper
helper = dbtHelper('snowflake')
print(helper.env_var('SNOWFLAKE_ACCOUNT', 'default-account'))
```

## Migration from Previous Versions

If you were using hardcoded paths in your profiles.yml:

### Before

```yaml
my_profile:
  outputs:
    dev:
      type: snowflake
      account: my-hardcoded-account
      user: my-hardcoded-user
      # ... other settings
      project_folder: /hardcoded/path/to/project
```

### After

```yaml
my_profile:
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      # ... other settings
```

And set environment variables in your notebook:

```python
os.environ["SNOWFLAKE_ACCOUNT"] = "my-account"
os.environ["SNOWFLAKE_USER"] = "my-user"
os.environ["MAGICS_PROJECT_FOLDER"] = "/path/to/project"
```

This approach provides better security and flexibility while maintaining compatibility with existing setups.