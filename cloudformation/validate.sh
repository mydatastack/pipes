TEMPLATES=("main.yaml" "api-gateway.yaml")
for i in "${TEMPLATES[@]}"; do aws cloudformation validate-template --template-body file://"$i"; done
