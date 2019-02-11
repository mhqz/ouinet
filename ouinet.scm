(define-module (ouinet)
  #:use-module (guix licenses)
  #:use-module (guix packages)
  #:use-module (guix git-download)
  #:use-module (guix build-system cmake)
  #:use-module (gnu packages)
  #:use-module (gnu packages autotools)
  #:use-module (gnu packages base)
  #:use-module (gnu packages boost)
  #:use-module (gnu packages certs)
  #:use-module (gnu packages commencement)
  #:use-module (gnu packages compression)
  #:use-module (gnu packages golang)
  #:use-module (gnu packages ipfs)
  #:use-module (gnu packages texinfo)
  #:use-module (gnu packages tls)
  #:use-module (gnu packages libidn)
  #:use-module (gnu packages libunistring)
  #:use-module (gnu packages pkg-config)
  #:use-module (gnu packages gettext)
  #:use-module (gnu packages gnupg)
  #:use-module (gnu packages rsync)
  #:use-module (gnu packages serialization)
  #:use-module (gnu packages version-control))

(define-public ouinet
  (package
    (name "ouinet")
    (version "0.0")
    (source
      (origin
        (method git-fetch)
        (uri (git-reference
          (url "https://github.com/equalitie/ouinet.git")
          (commit "6105ecb037c94c244e9187c1860853c79eb042e2")
          (recursive? #t)))
        (file-name (git-file-name name version))
        (sha256
          (base32 "0b3qczz55yk27xd51zhfg0fr8v90i40ga7ph5fzkpd9fmadya65s"))))
    (build-system cmake-build-system)
    (inputs
     `(("pkg-config" ,pkg-config)
       ("autoconf" ,autoconf)
       ("automake" ,automake)
       ("git" , git)
       ("go" , go)
       ("gx" , gx)
       ("gx-go" , gx-go)
       ("libgcrypt" , libgcrypt)
       ("libidn" , libidn)
       ("libressl" , libressl)
       ("libtool" , libtool)
       ("libunistring" , libunistring)
       ("libgpg-error" , libgpg-error)
       ("nlohmann-json-cpp" , nlohmann-json-cpp)
       ("nss-certs" , nss-certs)
       ("patch" , patch)
       ("pkg-config" , pkg-config)
       ("gcc-toolchain" , gcc-toolchain)
       ("gettext" , gettext-minimal)
       ("rsync" , rsync)
       ("texinfo" , texinfo)
       ("unzip" , unzip)
       ("zlib" , zlib)
       ("boost-cxx14" , boost-cxx14)))
    (synopsis "")
    (description "")
    (home-page "https://github.com/equalitie/ouinet")
    (license expat)))
ouinet
