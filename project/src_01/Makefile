
.PHONY: help all clean step1 step2 step3 step4 step5 step6 step7 step8 step9

help:
	@echo "数据处理项目命令"
	@echo "=================="
	@echo "make all         - 运行所有步骤"
	@echo "make step1       - 运行步骤1: Excel转JSON"
	@echo "make step2       - 运行步骤2: 多树结构处理"
	@echo "make step3       - 运行步骤3: 梯度到实体转换"
	@echo "make step4       - 运行步骤4: 数据库构建"
	@echo "make step5       - 运行步骤5: 相似度比较"
	@echo "make step6       - 运行步骤6: LLM后处理相似度分析"
	@echo "make step7       - 运行步骤7: 特征到实体转换"
	@echo "make step8       - 运行步骤8: 内容到等级转换"
	@echo "make step9       - 运行步骤9: 生成Kafka输出数据"
	@echo "make clean       - 清理处理后的文件"

all:
	python -m src_01.main_control --all

step1:
	python -m src_01.main_control --step 1

step2:
	python -m src_01.main_control --step 2

step3:
	python -m src_01.main_control --step 3

step4:
	python -m src_01.main_control --step 4

step5:
	python -m src_01.main_control --step 5

step6:
	python -m src_01.main_control --step 6

step7:
	python -m src_01.main_control --step 7

step8:
	python -m src_01.main_control --step 8

step9:
	python -m src_01.main_control --step 9

clean:
	rm -f data/processed/*
	@echo "已清理处理后的文件"

run-from-step:
	@echo "Usage: make run-from-step STEP=3"
	@if [ -z "$(STEP)" ]; then \
		echo "请指定起始步骤，例如: make run-from-step STEP=3"; \
	else \
		python -m src_01.main_control --from-step $(STEP); \
	fi