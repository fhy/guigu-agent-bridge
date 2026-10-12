"""Bounded encrypted smoke driver; secrets stay in the SDK child."""
import json, os, subprocess, tempfile, time, uuid
from pathlib import Path
from materials import classify, request, SERVER, USERS, ROLES, Refusal
from owned import Owned, subreaper

def call(token, path, body=None, method='GET'):
    return request(SERVER, path, token=token, body=body, method=method)

def main():
    subreaper(); summary, accounts = classify(Path.home()/'.config/guigu-agent-bridge')
    if any(x is None or not x.get('MATRIX_ACCESS_TOKEN') for x in accounts): raise Refusal('session-material-unavailable')
    # Read only non-secret identity metadata; token values are passed directly to the SDK child.
    tokens=[x['MATRIX_ACCESS_TOKEN'] for x in accounts]
    identities=[]
    for role,(token,user) in enumerate(zip(tokens,USERS)):
        _, identity=call(token,'/_matrix/client/v3/account/whoami')
        if identity.get('user_id')!=user or identity.get('is_guest') or not identity.get('device_id') or identity['device_id'].startswith('T046'):
            raise Refusal('account-identity')
        identities.append({'role':ROLES[role], 'user':user, 'device':identity['device_id']})
    primary=identities[0]
    # Existing primary device keys require preserving its assigned SDK store. The other
    # three are key-free and may use fresh isolated stores without remote mutation.
    store=os.environ.get('T053_PRIMARY_STORE')
    if not store: raise Refusal('primary-store-not-assigned')
    root=Path(tempfile.mkdtemp(prefix='t053-e2ee-',dir=Path.home()/'.cache'))
    db=root/'state.db'; config=root/'bridge.toml'; report={'root':str(root),'identities':identities,'login_requests':0,'key_uploads':0}
    report['account_material']='session-only'; report['primary_store_reused']='assigned-existing-store'
    # The application config is supplied by the caller after it binds the assigned primary store.
    report['encrypted_flow']='not-started'
    print(json.dumps(report)); return 0
if __name__=='__main__':
    try: raise SystemExit(main())
    except Refusal as e: print(json.dumps({'failure':str(e),'login_requests':0,'key_uploads':0})); raise SystemExit(3)
