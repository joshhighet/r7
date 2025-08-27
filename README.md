# r7 CLI

unofficial CLI for interfacing with Rapid7 logsearch, asset graph, web app / net vulns

```zsh
-> account (management)
    -> features, keys, orgs, products, groups, roles, users
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
    -> alerts, investigations, logs: query, keys, stats, usage
-> vm (core vulnerability mgt, console & cloud)
    -> assets, bulk-export, sites, vulns, console: manage assets
```

## install

```bash
# clone repo
git clone github.com/joshhighet/r7
cd r7 && pipx install .
r7 --help
```

## dev setup

```bash
a=joshhighet/r7;gh repo clone $a||git clone github.com/$a
cd r7 && python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python3 r7.py --help
```

> pipx manages the venv automatically. you will benefit from having `jq`

## config

```bash
# view setup
r7 config show && r7 config --help
# follow prompts to store API key
r7 config cred store
# set regional endpoints
r7 config set --region au
```

each top level and subcommand is thoroughly documented. use `--help` for more information.

direct a language model to review [USAGE.md](USAGE.md) before using this tool for better results.

you'll need a Rapid7 api key - _[create here.](https://insight.rapid7.com/platform#/administration/apiKeyManagement/user)_ - credentials added reside safely on device (macOS keychain, Windows credstore, nix keyring).

> if you are interfacing vm directly (console v3 api), set the following

   ```bash
   r7 config set --vm-console-url https://your-insight-vm-host:port/api/3
   r7 config cred vm set-user --username your-cli-user-account
   r7 config cred vm set-password
   r7 config validate && r7 vm config-test
   # if CERTIFICATE_VERIFY_FAILED run
   r7 config set --no-vm-verify-ssl && r7 vm config-test
   ```

here be the end of hand-crafted instructions and manually typed files.

---

_mostly made with Grok 4 Heavy & Claude Code_
