# Fase 2 — NF-e real (SEFAZ)

## Fluxo
1) ERP monta payload e chama o Serviço Fiscal
2) Serviço Fiscal:
   - (nesta versão) transmite `xml_nfe` para SEFAZ (NFeAutorizacao4)
   - consulta recibo (NFeRetAutorizacao4)
3) ERP grava `fiscal_documentos` com status, chave e protocolo

## Endpoints
- SP possui URLs específicas de homologação/produção para NFeAutorizacao4 / NFeRetAutorizacao4. (vide site SEFAZ/SP)
- Vários estados utilizam a SVRS para os serviços principais de NF-e. (Portal Nacional NF-e)
