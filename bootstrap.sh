#!/bin/sh

HERE=`readlink -m $0 | xargs dirname`
WAF_DIR="${HERE}/tool/waf"

EXTRAS=`ls -1 ${WAF_DIR}/waflib/extras/ | grep "\.py$" | sed 's/\.py$//g' | tr "\n\r" "," | sed 's/,$//g'`
if [ -e "${HERE}/waf" ]; then rm -f ${HERE}/waf; fi
(
    if [ -e "${HERE}/waf" ]; then rm -f ${HERE}/waf; fi && \
    if [ -e "${WAF_DIR}/waf" ]; then rm -f ${WAF_DIR}/waf; fi && \
    cd ${WAF_DIR} && \
    ./waf-light --make-waf --tools=${EXTRAS} configure build && \
    mv ${WAF_DIR}/waf ${HERE}/waf && \
    ./waf-light clean distclean
)
