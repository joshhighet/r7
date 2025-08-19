# r7 CLI

unofficial CLI for logsearch, asset graph, web app / net vulns

```zsh
-> account (management)
    -> features, orgs, keys, products, groups, roles, users
-> appsec (web app scans, findings)
    -> app+scan,list/get
-> asm (surface command cypher queries, apps/sdk)
    -> cypher, apps, profile, sdk
-> config (manage local configuration)
    -> cache, cred, reset, set, show, test, validate
-> docs(search dev docs)
    -> query
-> ic (manage automation jobs, execute workflows)
    -> artifacts, jobs, workflows
-> siem (search logs, manage alerts/investigations)
    -> logs, alerts, investigations
-> vm (core vulnerability mgt, console & cloud)
    -> assets, console, scans, sites, vulns
```

## setup

```bash
# clone repo
a=joshhighet/r7;gh repo clone $a||git clone github.com/$a
# create venv and setup python
python3 -m venv r7/.venv && cd r7 \
&& source .venv/bin/activate \
&& pip install -r requirements.txt
# run
./r7 --help
```

> ./r7 is a zsh wrapper for r7.py. setup a venv to get going. you will benefit from having `jq` - optionally add r7 to your PATH with `echo alias r7=$PWD/r7>>~/.zshrc`

each top level and subcommand is thoroughly documented. use `--help` for more information. dev/test only done on mac, changes needed for nix/win support.

you'll need a Rapid7 api key - _[create here.](https://insight.rapid7.com/platform#/administration/apiKeyManagement/user)_

```bash
# view setup
r7 config show && r7 config --help
# follow prompts to store API key
r7 config cred store
# set regional endpoints
r7 config set --region au
```

view with `r7 config show` - credentials sit in macOS Keychain. PR's are welcome for xplatform support.

if you want to globally invoke this, add it to your path. choose your own destiny - this is one way, assuming you cloned the repo to ~/Documents/GitHub/r7

   ```zsh
   alias r7=~/Documents/GitHub/r7/r7
   ```


> if you are interfacing vm directly (console v3 api), set the following

   ```bash
   r7 config set --vm-console-url https://your-insight-vm-host:port/api/3
   r7 config cred vm set-user --username your-cli-user-account
   r7 config cred vm set-password
   r7 config validate && r7 vm config-test
   # if CERTIFICATE_VERIFY_FAILED run
   r7 config set --no-vm-verify-ssl && r7 vm config-test
   ```

_here be the end of hand-crafted instructions and manually typed files. mostly made with Grok 4 Heavy & Claude Code_
