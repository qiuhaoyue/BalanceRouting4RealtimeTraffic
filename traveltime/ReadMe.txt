SpliteDualWay����˫����ȫ����Ϊ�����ߡ���Ҫ���������õ�·�ı�����main�������Լ����õ�����������ʻ��Ȩֵ����splteway�����У�
����ִ����ɺ���ԭ���н����е�˫���߸�Ϊ���������ߡ�


RoadCostUpdater�����ݲ�ͬ��·�ļ����޸ĵ�·�ļ�Ȩ���ȣ���Ϊmapmatching�м���transition weight�����ݡ���Ҫ��main�����и�����·��ͼ�ı�����


RemoveStopPoint����ǳ���ͣ�ڵ㡣����taxi sample�����Ϊ��������label_stop�������޸���������interval_threshold,distance_threshold.
����ִ�н�������taxi sample�������һ�У��������������ͣ�0=������1=Longstop



DBconnector: ��mapmatching��������ֻ��Ҫ�������⳵sample������ݿ�����ƣ�������Ҫ��Main���������õ�·��ͼ�ı�������intersection�ı����������ؼ�������mapMatching�����У�������distance_threshold(�㵽��·�ľ�����ֵ),temp_stop_threshold������Ʋ��Զ�ĵ�֮���·����, travel_threshold��·�����Ⱥ�ֱ�߾����ֵ�����ֵ��,time_threshold��������֮������ʱ��������

����ִ������Ժ����ÿ��sample�еĺ�����ӣ�Gid��Edge_offset,route�����ֶΣ��ֱ��ʾ�������ƥ���λ���Լ�������ǰһ���㵽��ǰsampleͨ����·����



RoadPassStat��ʹ�õ�ͼƥ��Ľ�����ü�ʱ�ٶȵ�ƽ��ֵ�����·��ƽ���ٶȣ��������ַ�ʽ�ò���ƽ���ٶȵĵ�·��ʹ��ͬ�ȼ���·��ƽ��ֵ��Ϊȱʡֵ������������к�õ��ĵ�·��ƽ���ٶȽ���Ϊ��һ��Travel Time Allocation�Ĳο���



TravelTimeAllocation��
��Ҫ����Sample table��roadmap table�����table������Ҫ�г�ʼ���ٶ���Ϣ������Ҫ����end_utc,cur_utc��Ȼ����������·��ͨ��ʱ�䲢�Ҵ������ݿ��С�allocate_time�Ὣ�������ڱ�$allocation_table+seq_num�У�aggregate_time�����Ὣ��������$time_table+seq_num�С���$allocation_table+seq_num�л�������ͨ��ʱ��һ����ʵ��ʹ��sample��������ģ�reference_count>0������һ����reference_count=0�ģ���һ��Ŀǰʹ�õĽ���ǡ�TripFinder:
get_trips������sample table�������table���ҳ����е�trip��������������һ���µı��С����Ե��ڵĲ�������temp_stop�ı��������trip���ȵȡ�
��trip�ҳ��Ժ󣬵���time_evaluation��������ͨ��ʱ�䡣





