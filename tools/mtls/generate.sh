#!/bin/bash -e

# In this script an intermediate certificate is created from a root certificate

if [[ $1 = "cleanup" ]]; then
  rm -rf 1_root
  rm -rf 2_intermediate
  rm -rf 3_application
  rm -rf 4_client

  exit 0
fi

if [[ $1 = "" ]]; then
  echo "please specify a domain ./generate.sh www.example.com"
  exit 1
fi

if [[ $2 == "" ]]; then
  echo "please specify a password for the private key"
  exit 1
fi

export SAN=DNS:$1

echo
echo Generate the root key
echo ---
mkdir -p 1_root/private
# Root private key generation.
openssl genrsa -aes128 -passout pass:$2 -out 1_root/private/ca.key.pem 3072
chmod 444 1_root/private/ca.key.pem


echo
echo Generate the root certificate
echo ---
mkdir -p 1_root/certs
mkdir -p 1_root/newcerts
touch 1_root/index.txt
echo "100212" > 1_root/serial
# Certificate request generation
# req - PKCS#10 certificate request and certificate generating utility
# -config openssl.cnf see file
# -new
# This option generates a new certificate request. It will prompt the user for the relevant field values.
# The actual fields prompted for and their maximum and minimum sizes are specified in the configuration file
# and any requested extensions.
# - key If the -key option is not used it will generate a new RSA private key using information specified in the
# configuration file.
# -extensions see openssl.cnf
# -subj arg Sets subject name for new request or supersedes the subject name when processing a request.  The arg must
# be formatted as /type0=value0/type1=value1/type2=..., characters may be escaped by \ (backslash), no
# spaces are skipped.
# -subj:
# Country Name (2 letter code) [AU]:GB
# State or Province Name (full name) [Some-State]:.
# Locality Name (eg, city) []:London
# Organization Name (eg, company) [Internet Widgits Pty Ltd]:Feisty Duck Ltd
# Organizational Unit Name (eg, section) []:
# Common Name (e.g. server FQDN or YOUR name) []:www.feistyduck.com
# Email Address []:webmaster@feistyduck.com
# Please enter the following 'extra' attributes
# to be sent with your certificate request
# A challenge password []:
# An optional company name []:
openssl req -config openssl.cnf \
      -key 1_root/private/ca.key.pem \
      -passin pass:$2 \
      -new -x509 -days 7300 -sha256 -extensions v3_ca \
      -subj "/C=US/ST=Denial/L=Springfield/O=Dis/CN=$1" \
      -out 1_root/certs/ca.cert.pem


echo
echo Verify root key
echo ---
# x509 - Certificate display and signing utility
# -noout This option prevents output of the encoded version of the request
# -text
# Prints out the certificate in text form. Full details are output including the public key, signature
# algorithms, issuer and subject names, serial number any extensions present and any trust settings.
openssl x509 -noout -text -in 1_root/certs/ca.cert.pem

echo
echo Generate the key for the intermediary certificate
echo ---
mkdir -p 2_intermediate/private
# Intermediate private key
openssl genrsa -aes128 \
  -passout pass:$2 \
  -out 2_intermediate/private/intermediate.key.pem 3072

chmod 444 2_intermediate/private/intermediate.key.pem


echo
echo Generate the signing request for the intermediary certificate
echo ---
mkdir -p 2_intermediate/csr
# Certificate request
# req - PKCS#10 certificate request and certificate generating utility
# -config openssl.cnf vedi configurazione allegata
# -new
# This option generates a new certificate request. It will prompt the user for the relevant field values.
# The actual fields prompted for and their maximum and minimum sizes are specified in the configuration file
# and any requested extensions.
# If the -key option is not used it will generate a new RSA private key using information specified in the
# configuration file.
openssl req -config openssl.cnf -new -sha256 \
      -passin pass:$2 \
      -subj "/C=US/ST=Denial/L=Springfield/O=Dis/CN=$1" \
      -key 2_intermediate/private/intermediate.key.pem \
      -out 2_intermediate/csr/intermediate.csr.pem


echo
echo Sign the intermediary
echo ---
mkdir -p 2_intermediate/certs
mkdir -p 2_intermediate/newcerts
touch 2_intermediate/index.txt
echo "100212" > 2_intermediate/serial

# Intermediate certificate sign
# The ca command is a minimal CA application. It can be used to sign certificate requests in a variety of forms
# and generate CRLs(Certificate Revocation List) it also maintains a text database of issued certificates and their status.
# -passin arg The key password source.
# -days arg The number of days to certify the certificate for.
# -notext Don't output the text form of a certificate to the output file.
# -md alg The message digest to use. Any digest supported by the OpenSSL dgst command can be used. This option also applies to
# -in filename An input filename containing a single certificate request to be signed by the CA.
openssl ca -config openssl.cnf -extensions v3_intermediate_ca \
        -passin pass:$2 \
        -days 3650 -notext -md sha256 \
        -in 2_intermediate/csr/intermediate.csr.pem \
        -out 2_intermediate/certs/intermediate.cert.pem

chmod 444 2_intermediate/certs/intermediate.cert.pem


echo
echo Verify intermediary
echo ---
openssl x509 -noout -text \
      -in 2_intermediate/certs/intermediate.cert.pem

openssl verify -CAfile 1_root/certs/ca.cert.pem \
      2_intermediate/certs/intermediate.cert.pem


echo
echo Create the chain file
echo ---
# Chain file creation
cat 2_intermediate/certs/intermediate.cert.pem \
      1_root/certs/ca.cert.pem > 2_intermediate/certs/ca-chain.cert.pem

chmod 444 2_intermediate/certs/ca-chain.cert.pem


echo
echo Create the application key
echo ---
mkdir -p 3_application/private
# Server private key
openssl genrsa \
      -passout pass:$2 \
    -out 3_application/private/$1.key.pem 3072

chmod 444 3_application/private/$1.key.pem


echo
echo Create the application signing request
echo ---
mkdir -p 3_application/csr
# Server certificate request is created from intermediate cert -> intermediate_openssl.cnf
# req - PKCS#10 certificate request and certificate generating utility
# -config openssl.cnf vedi configurazione allegata
# -new
# This option generates a new certificate request. It will prompt the user for the relevant field values.
# The actual fields prompted for and their maximum and minimum sizes are specified in the configuration file
# and any requested extensions.
# If the -key option is not used it will generate a new RSA private key using information specified in the
# configuration file.
# -subj arg Sets subject name for new request or supersedes the subject name when processing a request.  The arg must
# be formatted as /type0=value0/type1=value1/type2=..., characters may be escaped by \ (backslash), no
# spaces are skipped.
# -subj:
# Country Name (2 letter code) [AU]:GB
# State or Province Name (full name) [Some-State]:.
# Locality Name (eg, city) []:London
# Organization Name (eg, company) [Internet Widgits Pty Ltd]:Feisty Duck Ltd
# Organizational Unit Name (eg, section) []:
# Common Name (e.g. server FQDN or YOUR name) []:www.feistyduck.com
# Email Address []:webmaster@feistyduck.com
# Please enter the following 'extra' attributes
# to be sent with your certificate request
# A challenge password []:
# An optional company name []:
openssl req -config intermediate_openssl.cnf \
      -subj "/C=US/ST=Denial/L=Springfield/O=Dis/CN=$1" \
      -passin pass:$2 \
      -key 3_application/private/$1.key.pem \
      -new -sha256 -out 3_application/csr/$1.csr.pem


echo
echo Create the application certificate
echo ---
mkdir -p 3_application/certs
# Server certificate sign from intermediate
# The ca command is a minimal CA application. It can be used to sign certificate requests in a variety of forms
# and generate CRLs(Certificate Revocation List) it also maintains a text database of issued certificates and their status.
# -passin arg The key password source.
# -days arg The number of days to certify the certificate for.
# -notext Don't output the text form of a certificate to the output file.
# -md alg The message digest to use. Any digest supported by the OpenSSL dgst command can be used. This option also applies to
# -in filename An input filename containing a single certificate request to be signed by the CA.
openssl ca -config intermediate_openssl.cnf \
      -passin pass:$2 \
      -extensions server_cert -days 375 -notext -md sha256 \
      -in 3_application/csr/$1.csr.pem \
      -out 3_application/certs/$1.cert.pem

chmod 444 3_application/certs/$1.cert.pem


echo
echo Validate the certificate
echo ---
openssl x509 -noout -text \
      -in 3_application/certs/$1.cert.pem


echo
echo Validate the certificate has the correct chain of trust
echo ---
openssl verify -CAfile 2_intermediate/certs/ca-chain.cert.pem \
      3_application/certs/$1.cert.pem


echo
echo Generate the client key
echo ---
mkdir -p 4_client/private
# Create Client key
openssl genrsa \
    -passout pass:$2 \
    -out 4_client/private/$1.key.pem 3072

chmod 444 4_client/private/$1.key.pem


echo
echo Generate the client signing request
echo ---
mkdir -p 4_client/csr
# Client certification is created from intermediate cert -> intermediate_openssl.cnf
# req - PKCS#10 certificate request for client
openssl req -config intermediate_openssl.cnf \
      -subj "/C=US/ST=Denial/L=Springfield/O=Dis/CN=$1" \
      -passin pass:$2 \
      -key 4_client/private/$1.key.pem \
      -new -sha256 -out 4_client/csr/$1.csr.pem


echo
echo Create the client certificate
echo ---
mkdir -p 4_client/certs
# Client certificate sign from intermediate
# The ca command is a minimal CA application. It can be used to sign certificate requests in a variety of forms
# and generate CRLs(Certificate Revocation List) it also maintains a text database of issued certificates and their status.
# -passin arg The key password source.
# -days arg The number of days to certify the certificate for.
# -notext Don't output the text form of a certificate to the output file.
# -md alg The message digest to use. Any digest supported by the OpenSSL dgst command can be used. This option also applies to
# -in filename An input filename containing a single certificate request to be signed by the CA.
openssl ca -config intermediate_openssl.cnf \
      -passin pass:$2 \
      -extensions usr_cert -days 375 -notext -md sha256 \
      -in 4_client/csr/$1.csr.pem \
      -out 4_client/certs/$1.cert.pem

chmod 444 4_client/certs/$1.cert.pem
