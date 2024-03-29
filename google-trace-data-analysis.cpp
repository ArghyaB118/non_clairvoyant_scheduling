#include <iostream>
#include <algorithm> //for max
#include <string>
#include <sstream>
#include <fstream>
#include <unordered_map>
#include <queue>
#include <unordered_set>
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
	unsigned long long average = 0;
	count = 0;
	for (auto & i : task_ids) {
		if (i.second < min_length)
			min_length = i.second;
		if (i.second > max_length)
			max_length = i.second;
		count++; average += i.second;
	}
	average /= count;
	unsigned long long variance = 0;
	for (auto & i : task_ids)
		variance += (i.second - average) * (i.second - average);
	variance /= count;
	cout << "Shortest job length: " << min_length << endl;
	cout << "Largest job length: " << max_length << endl;
	cout << "Ratio of largest to smallest job length: " << (float)max_length / min_length << endl;
	cout << "Variance: " << variance << endl;
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

class CustomCompare {
public:
    bool operator() (pairULL p1, pairULL p2) {
        return task_ids[p1.second] > task_ids[p2.second];
    }
};

unsigned long long non_interruptive_opt () {
	int i = 0;
	unsigned long long horizon = stoi(arrival_times[i].first);
	unsigned long long total_completion_time = 0;
	priority_queue<pairULL, vector<pairULL>, CustomCompare> pq;
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
	priority_queue<pairULL, vector<pairULL>, CustomCompare> pq;
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

class custom_queue {
private:
	vector<pairULL> jobs;
	int executed;
public:
	custom_queue() {
		this->jobs = {};
		this->executed = 0;
	}
	void enqueue (pairULL p) {
		int n = rand() % (this->jobs.size() - this->executed + 1);
		this->jobs.insert(this->jobs.begin() + this->executed + n, p);
	}
	void execute () {
		this->executed++;
		// this->jobs.erase(this->jobs.begin(), this->jobs.begin() + 1);
	}
	pairULL top () {
		return this->jobs[this->executed];
	}
	bool empty() {
		if (this->jobs.size() == this->executed)
			return true;
		return false;
	}
	int size() {
		return this->jobs.size();
	}
	void dequeue (pairULL p) {
		std::remove (this->jobs.begin(), this->jobs.end(), p);
	}
};

unsigned long long non_interruptive_rjf () {
	int i = 0;
	unsigned long long horizon = stoi(arrival_times[i].first);
	unsigned long long total_completion_time = 0;
	custom_queue cq;
	while (i < arrival_times.size()) {
		// cout << i << endl;
		unsigned long long arrival_time = stoi(arrival_times[i].first);
		if (horizon >= arrival_time) {
			cq.enqueue({arrival_time, arrival_times[i].second});
			/*pairULL tmp;
			tmp.first = arrival_time;
			tmp.second = arrival_times[i].second;
			cq.enqueue(tmp);*/
			i++;
		}
		else {
			pairULL tmp = cq.top();
			horizon += task_ids[tmp.second];
			total_completion_time += (horizon - tmp.first);
			cq.execute();
		}
	}
	// cout << i << endl;
	while (!cq.empty()) {
		horizon += task_ids[cq.top().second];
		total_completion_time += (horizon - cq.top().first);
		cq.execute();
	}
	return total_completion_time;	
}

// error = 0 => consistent
unsigned long long non_interruptive_mla_rjf (float error) {
	double lambda = 0.5;
	int i = 0;
	unsigned long long horizonMLA = stoi(arrival_times[i].first);
	unsigned long long horizonRJF = stoi(arrival_times[i].first);
	unsigned long long total_completion_time = 0;
	custom_queue cq;
	priority_queue<pairULL, vector<pairULL>, Compare> pq;
	unordered_set<string> executed_ids;
	while (i < arrival_times.size()) {
		unsigned long long arrival_time = stoi(arrival_times[i].first);
		// cout << i << " " << arrival_time << " " << horizonMLA << " " << horizonRJF << endl;
		if (min(horizonMLA, horizonRJF) >= arrival_time) {
			cq.enqueue({arrival_time, arrival_times[i].second});
			pq.push({arrival_time, arrival_times[i].second});
			i++;
		}
		else if (horizonMLA <= horizonRJF) {
			while (true) {
				if (executed_ids.find(pq.top().second) != executed_ids.end())
					pq.pop();
				else
					break;
			}
			horizonMLA += (task_ids[pq.top().second] / lambda);
			total_completion_time += (horizonMLA - pq.top().first);
			executed_ids.insert(pq.top().second);
			pq.pop();	
		}
		else if (horizonRJF < horizonMLA) {
			// cout << cq.top().first << " " << cq.top().second << endl;
			while (true) {
				if (executed_ids.find(cq.top().second) != executed_ids.end())
					cq.execute();
				else
					break;
			}
			// cout << cq.top().first << cq.top().second << endl;
			pairULL tmp = cq.top();
			horizonRJF += (task_ids[tmp.second] / (1 - lambda));
			total_completion_time += (horizonMLA - tmp.first);
			executed_ids.insert(tmp.second);
			cq.execute();
		}
	}
	while (!cq.empty() && !pq.empty()) {
		if (horizonMLA <= horizonRJF && !pq.empty()) {
			// cout << "here" << pq.top().first << pq.top().second << endl;
			while (executed_ids.find(pq.top().second) != executed_ids.end())
				pq.pop();
			if (!pq.empty()) {
				horizonMLA += (task_ids[pq.top().second] / lambda);
				total_completion_time += (horizonMLA - pq.top().first);
				executed_ids.insert(pq.top().second);
				pq.pop();
			}
		}
		else if (horizonMLA > horizonRJF && !cq.empty()) {
			// cout << "heres" << cq.top().first << cq.top().second << endl;
			while (executed_ids.find(cq.top().second) != executed_ids.end())
				cq.execute();
			if (!cq.empty()) {
				horizonRJF += (task_ids[cq.top().second] / (1 - lambda));
				total_completion_time += (horizonRJF - cq.top().first);
				executed_ids.insert(cq.top().second);
				cq.execute();
			}
		}
	}
	// cout << pq.size() << " " << cq.size() << endl;
	if (!pq.empty()) {
		while (!pq.empty()) {
			if (executed_ids.find(pq.top().second) == executed_ids.end()) {
				horizonMLA += (task_ids[pq.top().second] / lambda);
				total_completion_time += (horizonMLA - pq.top().first);
				executed_ids.insert(pq.top().second);
			}
			pq.pop();
		}
	}
	if (!cq.empty()) {
		while (!cq.empty()) {
			if (executed_ids.find(cq.top().second) == executed_ids.end()) {
				horizonRJF += (task_ids[cq.top().second] / (1 - lambda));
				total_completion_time += (horizonRJF - cq.top().first);
				executed_ids.insert(cq.top().second);
			}
			cq.execute();
		}
	}
	return total_completion_time;
}

int main () {
	read_csv ("./google-trace/google-cluster-data-1.csv");
	unsigned long long C = 0;
	C = follow_arrival_order(); // 2412441911330
	cout << "Follow Arrival Order: " << C << endl;
	C = non_interruptive_opt(); // 514453773515 // 702009347060
	cout << "Non-interruptive OPT: " << C << endl;
	C = interruptive_opt(); // 514453773515 (CustomCompare) // 702009347060
	cout << "Interruptive OPT: " << C << endl;
	C = non_interruptive_rjf(); // 1558362353940
	cout << "Non-interruptive Random Job First: " << C << endl;
	//C = non_interruptive_mla_rjf(0.5); // 593102196350 // 829919915530
	cout << "Non-interruptive Consistent MLA-RJF: " << C << endl;
	return 0;
}