import boto3
import csv
from datetime import datetime, timedelta

def main():
    # 初始化客户端
    cloudtrail_client = boto3.client('cloudtrail')
    sts_client = boto3.client('sts')
    
    # 获取账号ID和区域
    account_id = sts_client.get_caller_identity()['Account']
    region = cloudtrail_client.meta.region_name
    
    # 设置时间范围（最近一个月）
    end_time = datetime.now()
    start_time = end_time - timedelta(days=360)
    
    # SNS写入操作列表
    sns_write_events = [
        'Publish',
        'PublishBatch',
        'CreateTopic',
        'DeleteTopic',
        'Subscribe',
        'Unsubscribe',
        'SetTopicAttributes',
        'AddPermission',
        'RemovePermission'
    ]
    
    # 生成CSV文件名
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"sns-write-operations-{account_id}-{region}-{timestamp}.csv"
    
    print(f"Querying SNS write operations from {start_time.strftime('%Y-%m-%d')} to {end_time.strftime('%Y-%m-%d')}...")
    
    all_events = []
    
    # 查询每个SNS写入事件
    for event_name in sns_write_events:
        print(f"Querying {event_name} events...")
        next_token = None
        
        while True:
            params = {
                'LookupAttributes': [
                    {
                        'AttributeKey': 'EventName',
                        'AttributeValue': event_name
                    }
                ],
                'StartTime': start_time,
                'EndTime': end_time
            }
            
            if next_token:
                params['NextToken'] = next_token
            
            try:
                response = cloudtrail_client.lookup_events(**params)
                events = response.get('Events', [])
                
                for event in events:
                    # 解析事件详情
                    event_detail = {
                        'EventTime': event.get('EventTime', '').strftime('%Y-%m-%d %H:%M:%S'),
                        'EventName': event.get('EventName', ''),
                        'UserName': event.get('Username', ''),
                        'SourceIPAddress': event.get('SourceIPAddress', ''),
                        'UserAgent': event.get('UserAgent', ''),
                        'ResourceName': '',
                        'ResourceType': '',
                        'ErrorCode': '',
                        'ErrorMessage': ''
                    }
                    
                    # 提取资源信息
                    resources = event.get('Resources', [])
                    if resources:
                        event_detail['ResourceName'] = resources[0].get('ResourceName', '')
                        event_detail['ResourceType'] = resources[0].get('ResourceType', '')
                    
                    # 检查是否有错误
                    cloud_trail_event = event.get('CloudTrailEvent', '{}')
                    if 'errorCode' in cloud_trail_event:
                        import json
                        try:
                            event_json = json.loads(cloud_trail_event)
                            event_detail['ErrorCode'] = event_json.get('errorCode', '')
                            event_detail['ErrorMessage'] = event_json.get('errorMessage', '')
                        except:
                            pass
                    
                    all_events.append(event_detail)
                
                next_token = response.get('NextToken')
                if not next_token:
                    break
                    
            except Exception as e:
                print(f"Error querying {event_name}: {str(e)}")
                break
    
    # 写入CSV文件
    if all_events:
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'EventTime', 'EventName', 'UserName', 'SourceIPAddress', 
                'UserAgent', 'ResourceName', 'ResourceType', 'ErrorCode', 'ErrorMessage'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # 按时间排序
            all_events.sort(key=lambda x: x['EventTime'], reverse=True)
            
            for event in all_events:
                writer.writerow(event)
        
        print(f"Found {len(all_events)} SNS write operations")
        print(f"Results saved to: {csv_filename}")
    else:
        print("No SNS write operations found in the specified time range")

if __name__ == "__main__":
    main()
