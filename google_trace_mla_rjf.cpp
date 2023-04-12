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

typedef pair<unsigned long long, string> pairULL;
class Compare {
public:
    bool operator() (pairULL p1, pairULL p2) {
        return p1.first < p2.first;
    }
};

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
	C = non_interruptive_mla_rjf(0.5); // 829919915530
	cout << "Non-interruptive Consistent MLA-RJF: " << C << endl;
	return 0;
}