package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// WorkflowDefinition is a persisted user-defined graph/workflow template.
// graph_json intentionally remains opaque so users can define their own fields.
type WorkflowDefinition struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Version     int       `json:"version"`
	GraphJSON   string    `json:"graph_json"`
	Enabled     bool      `json:"enabled"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type WorkflowRun struct {
	ID              string     `json:"id"`
	WorkflowID      string     `json:"workflow_id"`
	WorkflowVersion int        `json:"workflow_version"`
	ConversationID  string     `json:"conversation_id,omitempty"`
	ProjectID       string     `json:"project_id,omitempty"`
	RoleID          string     `json:"role_id,omitempty"`
	Status          string     `json:"status"`
	InputJSON       string     `json:"input_json,omitempty"`
	OutputJSON      string     `json:"output_json,omitempty"`
	Error           string     `json:"error,omitempty"`
	StartedAt       time.Time  `json:"started_at"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
}

type WorkflowNodeRun struct {
	ID         string     `json:"id"`
	RunID      string     `json:"run_id"`
	NodeID     string     `json:"node_id"`
	Status     string     `json:"status"`
	InputJSON  string     `json:"input_json,omitempty"`
	OutputJSON string     `json:"output_json,omitempty"`
	Error      string     `json:"error,omitempty"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

func scanWorkflowDefinition(scanner interface {
	Scan(dest ...interface{}) error
}) (*WorkflowDefinition, error) {
	var row WorkflowDefinition
	var desc sql.NullString
	var enabled int
	if err := scanner.Scan(&row.ID, &row.Name, &desc, &row.Version, &row.GraphJSON, &enabled, &row.CreatedAt, &row.UpdatedAt); err != nil {
		return nil, err
	}
	row.Description = desc.String
	row.Enabled = enabled != 0
	return &row, nil
}

const workflowDefinitionColumns = `id, name, description, version, graph_json, enabled, created_at, updated_at`

func (db *DB) ListWorkflowDefinitions(includeDisabled bool) ([]*WorkflowDefinition, error) {
	query := "SELECT " + workflowDefinitionColumns + " FROM workflow_definitions"
	if !includeDisabled {
		query += " WHERE enabled = 1"
	}
	query += " ORDER BY updated_at DESC"
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("查询工作流列表失败: %w", err)
	}
	defer rows.Close()

	var out []*WorkflowDefinition
	for rows.Next() {
		wf, err := scanWorkflowDefinition(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描工作流失败: %w", err)
		}
		out = append(out, wf)
	}
	return out, rows.Err()
}

func (db *DB) GetWorkflowDefinition(id string) (*WorkflowDefinition, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, nil
	}
	wf, err := scanWorkflowDefinition(db.QueryRow("SELECT "+workflowDefinitionColumns+" FROM workflow_definitions WHERE id = ?", id))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("查询工作流失败: %w", err)
	}
	return wf, nil
}

func (db *DB) UpsertWorkflowDefinition(wf *WorkflowDefinition) error {
	if wf == nil {
		return fmt.Errorf("工作流为空")
	}
	wf.ID = strings.TrimSpace(wf.ID)
	wf.Name = strings.TrimSpace(wf.Name)
	if wf.ID == "" || wf.Name == "" {
		return fmt.Errorf("工作流 id 和 name 不能为空")
	}
	if strings.TrimSpace(wf.GraphJSON) == "" {
		wf.GraphJSON = `{"nodes":[],"edges":[],"config":{}}`
	}
	if wf.Version <= 0 {
		wf.Version = 1
	}
	now := time.Now()
	existing, err := db.GetWorkflowDefinition(wf.ID)
	if err != nil {
		return err
	}
	if existing == nil {
		_, err = db.Exec(
			`INSERT INTO workflow_definitions (id, name, description, version, graph_json, enabled, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			wf.ID, wf.Name, wf.Description, wf.Version, wf.GraphJSON, boolToInt(wf.Enabled), now, now,
		)
	} else {
		nextVersion := existing.Version + 1
		if wf.Version > existing.Version {
			nextVersion = wf.Version
		}
		_, err = db.Exec(
			`UPDATE workflow_definitions
			 SET name = ?, description = ?, version = ?, graph_json = ?, enabled = ?, updated_at = ?
			 WHERE id = ?`,
			wf.Name, wf.Description, nextVersion, wf.GraphJSON, boolToInt(wf.Enabled), now, wf.ID,
		)
	}
	if err != nil {
		return fmt.Errorf("保存工作流失败: %w", err)
	}
	return nil
}

func (db *DB) DeleteWorkflowDefinition(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("工作流 id 不能为空")
	}
	if _, err := db.Exec("DELETE FROM workflow_definitions WHERE id = ?", id); err != nil {
		return fmt.Errorf("删除工作流失败: %w", err)
	}
	return nil
}

func (db *DB) CreateWorkflowRun(run *WorkflowRun) error {
	if run == nil {
		return fmt.Errorf("工作流运行为空")
	}
	if strings.TrimSpace(run.ID) == "" || strings.TrimSpace(run.WorkflowID) == "" {
		return fmt.Errorf("工作流运行 id 和 workflow_id 不能为空")
	}
	if run.WorkflowVersion <= 0 {
		run.WorkflowVersion = 1
	}
	if strings.TrimSpace(run.Status) == "" {
		run.Status = "running"
	}
	if run.StartedAt.IsZero() {
		run.StartedAt = time.Now()
	}
	_, err := db.Exec(
		`INSERT INTO workflow_runs (id, workflow_id, workflow_version, conversation_id, project_id, role_id, status, input_json, started_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		run.ID, run.WorkflowID, run.WorkflowVersion, nullString(run.ConversationID), nullString(run.ProjectID), nullString(run.RoleID), run.Status, run.InputJSON, run.StartedAt,
	)
	if err != nil {
		return fmt.Errorf("创建工作流运行失败: %w", err)
	}
	return nil
}

func (db *DB) FinishWorkflowRun(runID, status, outputJSON, errText string) error {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("工作流运行 id 不能为空")
	}
	if strings.TrimSpace(status) == "" {
		status = "completed"
	}
	now := time.Now()
	_, err := db.Exec(
		`UPDATE workflow_runs SET status = ?, output_json = ?, error = ?, finished_at = ? WHERE id = ?`,
		status, outputJSON, errText, now, runID,
	)
	if err != nil {
		return fmt.Errorf("更新工作流运行失败: %w", err)
	}
	return nil
}

func (db *DB) CreateWorkflowNodeRun(n *WorkflowNodeRun) error {
	if n == nil {
		return fmt.Errorf("工作流节点运行为空")
	}
	if strings.TrimSpace(n.ID) == "" || strings.TrimSpace(n.RunID) == "" || strings.TrimSpace(n.NodeID) == "" {
		return fmt.Errorf("节点运行 id、run_id 和 node_id 不能为空")
	}
	if strings.TrimSpace(n.Status) == "" {
		n.Status = "running"
	}
	if n.StartedAt.IsZero() {
		n.StartedAt = time.Now()
	}
	_, err := db.Exec(
		`INSERT INTO workflow_node_runs (id, run_id, node_id, status, input_json, started_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		n.ID, n.RunID, n.NodeID, n.Status, n.InputJSON, n.StartedAt,
	)
	if err != nil {
		return fmt.Errorf("创建工作流节点运行失败: %w", err)
	}
	return nil
}

func (db *DB) FinishWorkflowNodeRun(nodeRunID, status, outputJSON, errText string) error {
	nodeRunID = strings.TrimSpace(nodeRunID)
	if nodeRunID == "" {
		return fmt.Errorf("节点运行 id 不能为空")
	}
	if strings.TrimSpace(status) == "" {
		status = "completed"
	}
	now := time.Now()
	_, err := db.Exec(
		`UPDATE workflow_node_runs SET status = ?, output_json = ?, error = ?, finished_at = ? WHERE id = ?`,
		status, outputJSON, errText, now, nodeRunID,
	)
	if err != nil {
		return fmt.Errorf("更新工作流节点运行失败: %w", err)
	}
	return nil
}

func nullString(v string) interface{} {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	return v
}
