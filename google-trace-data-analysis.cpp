#include <iostream>
#include <algorithm> //for max
#include <string>
#include <sstream>
#include <fstream>
#include <unordered_map>
#include <queue>
using namespace std;
typedef pair<string, string> pairS;
// Global data containers
// stores the arrival time, task_id in strings
vector<pairS> arrival_times; 
// stores task_id and runtime in string, int
unordered_map<string, int> task_ids; 

void read_csv (string f) {
	fstream file (f, ios::in);
	string line, word;
	vector<string> row;
	if (file.is_open()) {
		while (getline(file, line)) {
			if (line[0] == 'T')
				continue;
			row.clear();
			stringstream s(line);
			while (getline(s, word, ' '))
				row.push_back(word);
			if (task_ids.find(row[2]) == task_ids.end()) {
				arrival_times.push_back({row[0], row[2]});
				task_ids[row[2]] = 5;
			}
			else {
				task_ids[row[2]] += 5;
			}
		}
	}
	cout << "To make sure that all the timestamps are divisible by 5" << endl;
	int count = 0, unsorted = 0;
	for (auto & i : arrival_times)
		if (stoi(i.first) % 5 != 0)
			count++;
	cout << "Count of timestamps not divisible by 5: " << count << endl;
	for (int i = 1; i < arrival_times.size(); i++)
		if (stoi(arrival_times[i].first) < stoi(arrival_times[i - 1].first))
			unsorted++;
	cout << "Number of unsorted jobs: " << unsorted << endl;
	cout << "Number of unique IDs: " << arrival_times.size() << endl;
	int min_length = task_ids[arrival_times[0].second], max_length = task_ids[arrival_times[0].second];
	for (auto & i : task_ids) {
		if (i.second < min_length)
			min_length = i.second;
		if (i.second > max_length)
			max_length = i.second;
	}
	cout << "Shortest job length: " << min_length << endl;
	cout << "Largest job length: " << max_length << endl;
	cout << "Ratio of largest to smallest job length: " << (float)max_length / min_length << endl;
}

unsigned long long follow_arrival_order () {
	int i = 0;
	// cout << arrival_times[i].first << endl;
	unsigned long long horizon = stoi(arrival_times[i].first);
	unsigned long long total_completion_time = 0;
	while (i < arrival_times.size()) {
		// max does not work, as unsigned long long vs int
		if (horizon < stoi(arrival_times[i].first))
			horizon = stoi(arrival_times[i].first);
		horizon += task_ids[arrival_times[i].second];
		total_completion_time += (horizon - stoi(arrival_times[i].first));
		i++;
	}
	return total_completion_time;
}

typedef pair<unsigned long long, string> pairULL;
class Compare {
public:
    bool operator() (pairULL p1, pairULL p2) {
        return p1.first < p2.first;
    }
};


unsigned long long non_interruptive_opt () {
	int i = 0;
	unsigned long long horizon = stoi(arrival_times[i].first);
	unsigned long long total_completion_time = 0;
	priority_queue<pairULL, vector<pairULL>, Compare> pq;
	while (i < arrival_times.size()) {
		// cout << arrival_times[i].first << endl;
		unsigned long long arrival_time = stoi(arrival_times[i].first);
		if (horizon >= arrival_time) {
			pairULL tmp;
			tmp.first = arrival_time;
			tmp.second = arrival_times[i].second;
			pq.push(tmp);
			i++;
		}
		else {
			horizon += task_ids[pq.top().second];
			total_completion_time += (horizon - pq.top().first);
			pq.pop();
		}
	}
	while (!pq.empty()) {
		horizon += task_ids[pq.top().second];
		total_completion_time += (horizon - pq.top().first);
		pq.pop();
	}
	return total_completion_time;
}

/*
|---|-------|-----------|	=> job x released at 0, job y released at \delta
0	\delta	y 			x  	=> job x is interrupted, job y done, then x done

|---|-------|-----------|	
0	\delta	x 	x-\delta+y 	=> job x is not interrupted, then job y done

Given, y < x. in fact, y < x-\delta. If SRPT is followed, x should be interrupted
However, preemption causes to lose the entire progress on the job.
Hence, SRPT is not optimal offline.
with interruption, total completion time, C = y+(x+y+\delta) = x+2y+\delta
without interruption, C = x+(x-\delta+y) = 2x+y-\delta
Condition when x should not be interrupted:
2x+y-\delta < x+2y+\delta => x < y+2\delta
Hence, y+\delta < x < y+2\delta, x should not be interrupted.
Otherwise, x should be interrupted.
*/

unsigned long long interruptive_opt () {
	int i = 0;
	unsigned long long horizon = stoi(arrival_times[i].first);
	unsigned long long total_completion_time = 0;
	priority_queue<pairULL, vector<pairULL>, Compare> pq;
	while (i < arrival_times.size()) {
		unsigned long long arrival_time = stoi(arrival_times[i].first);
		if (horizon >= arrival_time) {
			pairULL tmp;
			tmp.first = arrival_time;
			tmp.second = arrival_times[i].second;
			pq.push(tmp);
			i++;
		}
		// x = task_ids[pq.top().second]
		// \delta = stoi(arrival_times[i].first) - horizon
		// y = task_ids[arrival_times[i].second]
		else if ((horizon < arrival_time) && (task_ids[pq.top().second] >= (task_ids[arrival_times[i].second] + 2 * stoi(arrival_times[i].first) - horizon))) {
			pairULL tmp;
			tmp.first = arrival_time;
			tmp.second = arrival_times[i].second;
			pq.push(tmp);
			i++;
		}
		else {
			horizon += task_ids[pq.top().second];
			total_completion_time += (horizon - pq.top().first);
			pq.pop();
		}
	}
	while (!pq.empty()) {
		horizon += task_ids[pq.top().second];
		total_completion_time += (horizon - pq.top().first);
		pq.pop();
	}
	return total_completion_time;
}

int main () {
	read_csv ("./google-trace/google-cluster-data-1.csv");
	unsigned long long C = 0;
	C = follow_arrival_order();
	cout << "Follow Arrival Order: " << C << endl;
	C = non_interruptive_opt();
	cout << "Non-interruptive OPT: " << C << endl;
	C = interruptive_opt();
	cout << "Interruptive OPT: " << C << endl;
	return 0;
}