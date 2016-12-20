package main // import "github.com/nutmegdevelopment/singularity-client"

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	log "github.com/Sirupsen/logrus"
)

const (
	logTypeStdOut                = "stdout"
	logTypeStdErr                = "stderr"
	singularityHistoryURL        = "%s/api/history/tasks?requestId=%s&deployId=%s"
	singularityActiveTasksURL    = "%s/api/tasks/active"
	singularityLogURL            = "%s/api/sandbox/%s/read?path=%s&length=30000&offset=%d"
	singularityRequestURL        = "%s/api/requests"
	singularityDeployURL         = "%s/api/deploys"
	singularityTaskCompleteURL   = "%s/api/history/tasks?requestId=%s&deployId=%s"
	httpPostTimeout              = time.Duration(30 * time.Second)
	logRetryDelay                = 3000 * time.Millisecond
	taskIDRetryDelay             = 2000 * time.Millisecond
	getTaskIDRetryTimeoutSeconds = 120
)

var (
	requestJSONFile string
	deployJSONFile  string
	debug           bool
	singularityURL  string
	isComplete      = false
)

// SingularityRequest ...
type SingularityRequest struct {
	ID                                    string
	RequestType                           string
	Owners                                []string
	NumRetriesOnFailure                   int
	KillOldNonLongRunningTasksAfterMillis int
	RequiredSlaveAttributes               map[string]string
	ScheduledExpectedRuntimeMillis        int
}

// SingularityDeploy ...
type SingularityDeploy struct {
	Deploy struct {
		RequestID     string
		ID            string `json:"id"`
		Arguments     []string
		ContainerInfo interface{}
		Resources     interface{}
	}
}

// Call a singularity API endpoint (GET).
func getSingularityAPI(url string) ([]byte, error) {
	log.WithFields(log.Fields{
		"url": url,
	}).Debug("Calling singularity API")

	response, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("Error calling singularity API: %s", err)
	}

	defer response.Body.Close()
	if response.StatusCode == 200 {
		defer response.Body.Close()
		body, _ := ioutil.ReadAll(response.Body)
		return body, nil
	}

	return nil, fmt.Errorf("Non 200 status code returned (%d): %s", response.StatusCode, err)
}

// Call a singularity API endpoint (POST).
func postSingularityAPI(url string, postBody []byte) ([]byte, error) {
	httpVerb := "POST"

	log.WithFields(log.Fields{
		"url":       url,
		"http-verb": httpVerb,
		"body":      string(postBody),
	}).Debug("Posting JSON to singularity")

	req, err := http.NewRequest(httpVerb, url, bytes.NewBuffer(postBody))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: httpPostTimeout,
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Unable to complete singularity post: %s", err)
	}

	defer response.Body.Close()
	body, _ := ioutil.ReadAll(response.Body)
	if response.StatusCode == 200 {
		log.WithFields(log.Fields{
			"response-body:": string(body),
			"status-code":    response.StatusCode,
		}).Debug("Successfully posted to singularity")
		return body, nil
	}

	return nil, fmt.Errorf("Non 200 response from singularity post: %d - %s", response.StatusCode, string(body))
}

// ActiveTask ...
type ActiveTask struct {
	TaskID struct {
		RequestID       string
		DeployID        string
		StartedAt       int
		InstanceNo      int
		Host            string
		SanitizedHost   string
		SanitizedRackID string
		RackID          string
		ID              string
	}
}

// ActiveTasks ...
type ActiveTasks []ActiveTask

// RequestHistory ...
type RequestHistory struct {
	TaskID struct {
		RequestID       string
		DeployID        string
		StartedAt       int
		InstanceNo      int
		Host            string
		SanitizedHost   string
		SanitizedRackID string
		RackID          string
		ID              string
	}
	UpdatedAt     int
	LastTaskState string
}

func (r *RequestHistory) String() string {
	requestHistoryTemplate := "Deploy ID: %s, Task ID: %s"
	return fmt.Sprintf(requestHistoryTemplate, r.TaskID.DeployID, r.TaskID.ID)
}

// RequestHistoryItems ...
type RequestHistoryItems []RequestHistory

func getRequestHistory(requestID string, deployID string) (RequestHistory, error) {
	var requestHistoryItems RequestHistoryItems

	url := fmt.Sprintf(singularityHistoryURL, singularityURL, requestID, deployID)
	jsonResponse, err := getSingularityAPI(url)
	if err != nil {
		var requestHistory RequestHistory
		return requestHistory, fmt.Errorf("Error making API call: %s", err)
	}
	log.WithFields(log.Fields{
		"response": string(jsonResponse),
	}).Debug("Request history")

	err = json.Unmarshal(jsonResponse, &requestHistoryItems)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Unable to unmarshal json")
	}

	if len(requestHistoryItems) == 0 {
		var requestHistory RequestHistory
		return requestHistory, fmt.Errorf("RequestHistoryItems is empty")
	}

	return requestHistoryItems[0], nil
}

// Has this task completed?
// Completed is defined by a lastTaskState of:
//   TASK_FINISHED, TASK_FAILED, TASK_KILLED, TASK_LOST, TASK_LOST_WHILE_DOWN or TASK_ERROR
func hasTaskCompleted(requestID string, deployID string) bool {
	url := fmt.Sprintf(singularityTaskCompleteURL, singularityURL, requestID, deployID)
	response, err := getSingularityAPI(url)
	if err != nil {
		log.WithFields(log.Fields{
			"url":      url,
			"response": string(response),
		}).Error("Problem getting task complete status")
	}

	log.WithFields(log.Fields{
		"url":      url,
		"response": string(response),
	}).Debug("Task complete status")

	var requestHistoryItems RequestHistoryItems
	err = json.Unmarshal(response, &requestHistoryItems)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Unable to unmarshal task complete json")
	}

	if len(requestHistoryItems) == 1 {
		log.WithFields(log.Fields{
			"history-item":    requestHistoryItems[0],
			"last-task-state": requestHistoryItems[0].LastTaskState,
		}).Debug("Found request history item")

		switch requestHistoryItems[0].LastTaskState {
		case "TASK_FINISHED", "TASK_FAILED", "TASK_KILLED", "TASK_LOST", "TASK_LOST_WHILE_DOWN", "TASK_ERROR":
			log.WithFields(log.Fields{
				"state": requestHistoryItems[0].LastTaskState,
			}).Info("Task completed")
			return true
		}
	} else {
		log.WithFields(log.Fields{
			"found":    len(requestHistoryItems),
			"expected": 1,
		}).Debug("Incorrect number of request history items")
	}

	return false
}

// Depending on the lastTaskState did this complete without error?
func didTaskCompleteSuccessfully(requestID string, deployID string) bool {
	return true
}

// Gets the taskID from the active tasks endpoint.
// The history endpoint was used but it only returns an entry for the
// current task once it has completed.  As it can take a while for a
// task with log files to complete instead the active tasks endpoint is
// used and looped over to see if there is a match in there (based on
// requestID and deployID).
func getTaskID(requestID string, deployID string) (string, error) {
	url := fmt.Sprintf(singularityActiveTasksURL, singularityURL)
	response, err := getSingularityAPI(url)
	if err != nil {
		log.WithFields(log.Fields{
			"url":      url,
			"response": string(response),
		}).Error("Problem getting active tasks")
	}

	var activeTasks ActiveTasks
	err = json.Unmarshal(response, &activeTasks)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("Unable to unmarshal active tasks json")
	}

	log.WithFields(log.Fields{
		"url":          url,
		"response":     string(response),
		"active-tasks": activeTasks,
	}).Debug("Active tasks")

	for i := 0; i < len(activeTasks); i++ {
		if activeTasks[i].TaskID.RequestID == requestID && activeTasks[i].TaskID.DeployID == deployID {
			// We have a match!
			log.WithFields(log.Fields{
				"active-task": activeTasks[i],
				"task-id":     activeTasks[i].TaskID.ID,
			}).Info("Found task ID")
			return activeTasks[i].TaskID.ID, nil
		}
	}

	return "", fmt.Errorf("No matching taskId in the active tasks list")

	// curl -s 'https://singularity/api/tasks/active'
	// requestHistory, err := getRequestHistory(requestID, deployID)
	// if err != nil {
	// 	return "", fmt.Errorf("Unable to get the task ID: %s", err)
	// }
	// log.WithFields(log.Fields{
	// 	"taskID": requestHistory.TaskID.ID,
	// }).Info("Returning task ID")
	// return requestHistory.TaskID.ID, nil
}

// Keep polling for the taskID as it can take a few seconds before it is
// available.  If it takes longer than the retryDuration then abort the
// run.
func getTaskIDWithRetry(requestID string, deployID string, retryDuration int64) (string, error) {
	var taskID string
	var err error
	start := getNow()
	for {
		taskID, err = getTaskID(requestID, deployID)
		if err != nil {
			log.WithFields(log.Fields{
				"reason":    err,
				"time-left": fmt.Sprintf("%ds", getTimeLeft(start, retryDuration)),
			}).Info("TaskID not yet available. Retrying...")
		}

		if taskID != "" {
			break
		}

		now := getNow()
		// log.Printf("Start: %d, now: %d, retryDuration (seconds): %d", start, now, retryDuration)
		if now > (start + retryDuration) {
			return "", fmt.Errorf("Timed out trying to get the taskID")
		}

		time.Sleep(taskIDRetryDelay)
	}

	return taskID, nil
}

// Return the current time in seconds since epoch.
func getNow() int64 {
	return time.Now().UnixNano() / int64(time.Second)
}

func getTimeLeft(startTime int64, duration int64) int64 {
	return duration - (getNow() - startTime)
}

// SandboxLog ...
type SandboxLog struct {
	Data   string
	Offset int
}

// Gets the stdout log.
func getStdOutLog(ID string, offset int) (SandboxLog, error) {
	return getLog(ID, offset, logTypeStdOut)
}

// Gets the stderr log.
func getStdErrLog(ID string, offset int) (SandboxLog, error) {
	return getLog(ID, offset, logTypeStdErr)
}

// Does the actual work of getting a log entry for either stderr or
// stdout.
func getLog(ID string, offset int, logType string) (SandboxLog, error) {
	var sandboxLog SandboxLog

	url := fmt.Sprintf(singularityLogURL, singularityURL, ID, logType, offset)
	jsonResponse, err := getSingularityAPI(url)
	if err != nil {
		return sandboxLog, fmt.Errorf("Unable to retrieve the log")
	}
	// log.Printf("json: %s", jsonResponse)

	err = json.Unmarshal(jsonResponse, &sandboxLog)
	if err != nil {
		return sandboxLog, fmt.Errorf("Unable to unmarshal the log")
	}
	return sandboxLog, nil
}

// Like tail -f this continuously looks for updates to the stdout and
// stderr logs.
// Once the task is in a completed state it looks for logs on last
// time and then exits (returns).
func tailLogs(ID string, requestID string, deployID string) {
	var offsetStdErr int
	var offsetStdOut int
	for {

		isComplete = hasTaskCompleted(requestID, deployID)

		sandboxLog, err := getStdOutLog(ID, offsetStdOut)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("Error getting stdout log data")
		} else {
			if len(sandboxLog.Data) > 0 {
				fmt.Print(sandboxLog.Data)
				offsetStdOut += len(sandboxLog.Data)
			}
		}

		sandboxLog, err = getStdErrLog(ID, offsetStdErr)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err,
			}).Error("Error getting stderr log data")
		} else {
			if len(sandboxLog.Data) > 0 {
				fmt.Print(sandboxLog.Data)
				offsetStdErr += len(sandboxLog.Data)
			}
		}

		// If this deploy is now complete then return.
		if isComplete {
			log.WithFields(log.Fields{
				"is-complete": isComplete,
			}).Info("Job complete!")
			return
		}

		time.Sleep(logRetryDelay)
	}
}

// Create a singularity request.
func createRequest(requestJSON []byte, requestID string) error {
	url := fmt.Sprintf(singularityRequestURL, singularityURL)
	responseJSON, err := postSingularityAPI(url, requestJSON)
	if err != nil {
		log.WithFields(log.Fields{
			"response":   string(responseJSON),
			"request-id": requestID,
		}).Debug("Error creating request")
		return err
	}

	log.Info("Created request")

	return nil
}

// Create a singularity deploy.
func createDeploy(deployJSON []byte, deployID string) error {
	url := fmt.Sprintf(singularityDeployURL, singularityURL)
	responseJSON, err := postSingularityAPI(url, deployJSON)
	if err != nil {
		log.WithFields(log.Fields{
			"response":  string(responseJSON),
			"deploy-id": deployID,
		}).Debug("Error creating deploy")
		return err
	}

	log.Info("Created deploy")

	return nil
}

// Read in a file.
func readFile(filename string) ([]byte, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Read in a file and fatal error if there is a problem.
func readFileOrDie(filename string) []byte {
	b, err := readFile(filename)
	if err != nil {
		log.Fatalf("Unable to read file: %s. %s", filename, err)
	}
	return b
}

// Returns the deployID from the deploy config JSON.
func getDeployID(config []byte) (string, error) {
	log.WithFields(log.Fields{
		"json": string(config),
	}).Debug("Deploy JSON")

	var singularityDeploy SingularityDeploy
	err := json.Unmarshal(config, &singularityDeploy)
	if err != nil {
		return "", fmt.Errorf("Unable to unmarshal the deploy")
	}

	log.WithFields(log.Fields{
		"deploy-id": singularityDeploy.Deploy.ID,
	}).Debug("Deploy ID from config")

	return singularityDeploy.Deploy.ID, nil
}

// Returns the requestID from the deploy config JSON.
func getRequestID(config []byte) (string, error) {
	log.WithFields(log.Fields{
		"json": string(config),
	}).Debug("Deploy JSON")

	var singularityDeploy SingularityDeploy
	err := json.Unmarshal(config, &singularityDeploy)
	if err != nil {
		return "", fmt.Errorf("Unable to unmarshal the deploy")
	}

	log.WithFields(log.Fields{
		"deploy-id": singularityDeploy.Deploy.ID,
	}).Debug("Deploy ID from config")

	return singularityDeploy.Deploy.RequestID, nil
}

func main() {
	flag.BoolVar(&debug, "debug", false, "debug output.")
	flag.StringVar(&singularityURL, "singularity-url", "", "The singularity server url.")
	flag.StringVar(&deployJSONFile, "deploy-json", "", "The deploy JSON file.")
	flag.StringVar(&requestJSONFile, "request-json", "", "The request JSON file.")
	flag.Parse()

	if requestJSONFile == "" || deployJSONFile == "" || singularityURL == "" {
		log.Fatal("Missing required arguments - run 'singularity-client -h' for argument details.")
	}

	// read in the two required config files.
	requestJSON := readFileOrDie(requestJSONFile)
	deployJSON := readFileOrDie(deployJSONFile)

	deployID, err := getDeployID(deployJSON)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Unable to get deploy id")
	}

	requestID, err := getRequestID(deployJSON)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Unable to get request id")
	}

	err = createRequest(requestJSON, requestID)
	if err != nil {
		log.WithFields(log.Fields{
			"error":      err,
			"request-id": requestID,
		}).Fatal("Error creating the request")
	}

	err = createDeploy(deployJSON, deployID)
	if err != nil {
		log.WithFields(log.Fields{
			"error":      err,
			"request-id": requestID,
			"deploy-id":  deployID,
		}).Fatal("Error creating the deploy")
	}

	//
	taskID, err := getTaskIDWithRetry(requestID, deployID, getTaskIDRetryTimeoutSeconds)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("Unable to get the taskID so cannot continue!")
	}

	// This is the last step so once log tailing has ended then the
	// whole process has effectively ended.
	tailLogs(taskID, requestID, deployID)
}
