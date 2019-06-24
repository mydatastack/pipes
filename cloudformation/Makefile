TEMPLATES=main collector-pipes-apig collector-ga

# Google Duplicator Deplyoment Variables
GA_STACK_FILE=google-analytics-collector.yaml
BUCKET=us-east-1-pipes-cf-artifacts
GA_STACKNAME=tarasowski-pipes-dev-machine
#-----

validate:
	@for i in $(TEMPLATES); do \
		aws cloudformation validate-template --template-body file://$$i.yaml; \
		done
	@echo All cloudfromation files are valid

ga_deploy: validate
	@if [ ! -d 'temp/ga' ]; then \
		 mkdir -p temp/ga; \
	fi
	@rm -rf temp/ga && mkdir -p temp/ga
	@aws cloudformation package --template-file $(GA_STACK_FILE) --output-template-file temp/ga/output.yaml --s3-bucket $(BUCKET) --region us-east-1
	@aws cloudformation deploy --template-file temp/ga/output.yaml --stack-name $(GA_STACKNAME) --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND --region us-east-1
