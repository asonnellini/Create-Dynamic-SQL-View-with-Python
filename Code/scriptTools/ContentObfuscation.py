import cryptography.fernet as f
import base64


FernetKey = b'dHBOVFNaZ0tfV2ctY1ZqN1NVSG5ZekdZY2xlOGw1MkR3Y2RZR2JFWDdYVT0='

obfuscator = f.Fernet(base64.b64decode(FernetKey))

def obfuscate(clearText:str):
    """
    This function obfuscate the input string

    Input:
        - clearText: input string to be obfuscated    
    """
    return (obfuscator.encrypt(clearText.encode())).decode()


def deobfuscate(obfText:str):
    """
    This function obfuscate the input string

    Input:
        - obfText: input string to be obfuscated    
    """
    return (obfuscator.decrypt(obfText.encode())).decode()
