"""
Python Version 3.9 설정
Job Parameter : --additional-python-modules , pgpy==0.6.0
                --python-modules-installer-option , install
"""
import pgpy
import boto3

s3_client = boto3.client('s3')

my_public_key = """-----BEGIN PGP PUBLIC KEY BLOCK-----
-----END PGP PUBLIC KEY BLOCK-----"""

your_public_key = """-----BEGIN PGP PUBLIC KEY BLOCK-----
-----END PGP PUBLIC KEY BLOCK-----"""

#S3에서 평문 파일을 읽는다.
response = s3_client.get_object(
    Bucket = 'bucket-name',
    Key = 'folder/filename.csv'
)

#PGP암호화 한다.
encryption_key,_ = pgpy.PGPKey.from_blob(your_public_key)
encryption_key._require_usage_flags = False
message = pgpy.PGPMessage.new(response['Body'].read().decode('utf-8'))
enc_message = encryption_key.pubkey.encrypt(message)
print(enc_message)

#암호화된 파일을 S3에 저장한다.
s3_client.put_object(Body=bytes(enc_message), Bucket='bucket-name', Key='folder/filename.csv.pgp')
print('Encrypt File Successfully.')


my_private_key = """-----BEGIN PGP PRIVATE KEY BLOCK-----
-----END PGP PRIVATE KEY BLOCK-----"""

#S3에서 암호화된 파일을 읽는다.
response = s3_client.get_object(
    Bucket = 'bucket-name',
    Key = 'folder/filename.csv.pgp'
)
#복호화 한다.
decryption_key, _ = pgpy.PGPKey.from_blob(my_private_key)
PASSPHRASE = "test_pass"
encrypted_message = pgpy.PGPMessage.from_blob(response['Body'].read())

with decryption_key.unlock(PASSPHRASE):
    decrypted_message = decryption_key.decrypt(encrypted_message)

print(decrypted_message.message)
#평문 파일을 S3에 저장한다.
s3_client.put_object(Body=decrypted_message.message, Bucket='bucket-name', Key='folder/decrypted_filename.csv')
print('Decrypt File Successfully.')

