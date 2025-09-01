import boto3
import json
import csv
from datetime import datetime

def get_sns_topics(sns_client):
    """获取账号中所有 SNS 主题"""
    topics = []
    next_token = None
    while True:
        response = sns_client.list_topics(NextToken=next_token) if next_token else sns_client.list_topics()
        topics.extend(response.get('Topics', []))
        next_token = response.get('NextToken')
        if not next_token:
            break
    return topics

def get_subscriptions_for_topic(sns_client, topic_arn):
    """获取指定 SNS 主题的订阅者"""
    subscriptions = []
    next_token = None
    while True:
        response = sns_client.list_subscriptions_by_topic(
            TopicArn=topic_arn,
            NextToken=next_token
        ) if next_token else sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
        subscriptions.extend(response.get('Subscriptions', []))
        next_token = response.get('NextToken')
        if not next_token:
            break
    return subscriptions

def get_topic_policy(sns_client, topic_arn):
    """获取 SNS 主题的访问策略以推测发布者"""
    try:
        response = sns_client.get_topic_attributes(TopicArn=topic_arn)
        policy = response.get('Attributes', {}).get('Policy', '{}')
        return json.loads(policy)
    except sns_client.exceptions.NotFoundException:
        return {"Error": "Topic policy not found"}
    except Exception as e:
        return {"Error": str(e)}

def main():
    # 初始化 SNS 客户端
    sns_client = boto3.client('sns')
    
    # 获取账号ID和区域
    sts_client = boto3.client('sts')
    account_id = sts_client.get_caller_identity()['Account']
    region = sns_client.meta.region_name
    
    # 生成时间戳
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 生成CSV文件名
    csv_filename = f"{account_id}-{region}-{timestamp}.csv"

    # 获取所有 SNS 主题
    topics = get_sns_topics(sns_client)
    if not topics:
        print("No SNS topics found in the account.")
        return

    # 写入CSV文件
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['TopicArn', 'SubscriptionArn', 'Protocol', 'Endpoint', 'Publishers']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # 遍历每个主题
        for topic in topics:
            topic_arn = topic['TopicArn']

            # 获取订阅者
            subscriptions = get_subscriptions_for_topic(sns_client, topic_arn)
            
            # 获取发布者信息
            policy = get_topic_policy(sns_client, topic_arn)
            publishers = []
            if "Statement" in policy:
                for statement in policy.get("Statement", []):
                    if statement.get("Effect") == "Allow" and "sns:Publish" in statement.get("Action", []):
                        publishers.append(str(statement.get("Principal", "")))
            
            publishers_str = "; ".join(publishers) if publishers else ""

            # 如果没有订阅者，至少写入主题信息
            if not subscriptions:
                writer.writerow({
                    'TopicArn': topic_arn,
                    'SubscriptionArn': '',
                    'Protocol': '',
                    'Endpoint': '',
                    'Publishers': publishers_str
                })
            else:
                # 为每个订阅者写入一行
                for sub in subscriptions:
                    writer.writerow({
                        'TopicArn': topic_arn,
                        'SubscriptionArn': sub.get('SubscriptionArn', ''),
                        'Protocol': sub.get('Protocol', ''),
                        'Endpoint': sub.get('Endpoint', ''),
                        'Publishers': publishers_str
                    })

    print(f"Results saved to: {csv_filename}")

if __name__ == "__main__":
    main()