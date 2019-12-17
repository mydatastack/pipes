TEMPLATES = template api.template firehose.template 

CFN_BUCKET :=pipes-cf-artifacts
STACK_NAME := tarasowski-pipes-local-test-2
STAGE := local
NAME := pipes

deploy:
	aws cloudformation package --template ./infrastructure/app/template.yaml --s3-bucket $(CFN_BUCKET) --output json > ./infrastructure/app/output.json
	aws cloudformation deploy --template-file ./infrastructure/app/output.json --stack-name $(STACK_NAME) --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --region eu-central-1 --parameter-overrides STAGE=$(STAGE) NAME=$(NAME)


validate:
	@for i in $(TEMPLATES); do \
		aws cloudformation validate-template --template-body file://infrastructure/app/$$i.yaml; \
		done
	@echo All cloudfromation files are valid
