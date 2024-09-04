package kvclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
)

type KVClient struct {
	BaseURL string
}

type KVResult struct {
    Keys        []string            `json:"keys"`
    NextCursor  string              `json:"nextCursor"`
}

func NewKVClient(host string, port int) *KVClient {
	return &KVClient{
		BaseURL: fmt.Sprintf("http://%s:%d", host, port),
	}
}

func (c *KVClient) Set(key, value string) error {
    data := map[string]string{
        key: value,
    }
    jsonData, err := json.Marshal(data)
    if err != nil {
        return fmt.Errorf("failed to marshal JSON: %v", err)
    }

    req, err := http.NewRequest("POST", c.BaseURL+"/set", bytes.NewBuffer(jsonData))
    if err != nil {
        return fmt.Errorf("failed to create request: %v", err)
    }
    req.Header.Set("Content-Type", "application/json")

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return fmt.Errorf("failed to send request: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
    }

    return nil
}


func (c *KVClient) BatchOperation(pairs []KVPair) error {
    jsonData, err := json.Marshal(pairs)
    if err != nil {
        return fmt.Errorf("failed to marshal JSON: %v", err)
    }

    req, err := http.NewRequest("POST", c.BaseURL+"/batch", bytes.NewBuffer(jsonData))
    if err != nil {
        return fmt.Errorf("failed to create request: %v", err)
    }
    req.Header.Set("Content-Type", "application/json")

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return fmt.Errorf("failed to send request: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        body, _ := io.ReadAll(resp.Body)
        return fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
    }

    return nil
}


func (c *KVClient) Get(key string) (string, error) {
	resp, err := http.Get(fmt.Sprintf("%s/get?key=%s", c.BaseURL, url.QueryEscape(key)))
	if err != nil {
		return "", fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("failed to read response body: %v", err)
		}

		var result string
		if err := json.Unmarshal(body, &result); err != nil {
			return "", fmt.Errorf("failed to unmarshal response: %v", err)
		}

		return result, nil

	case http.StatusNoContent:
		return "", nil // 데이터가 없음을 나타내기 위해 빈 문자열과 nil 에러 반환

	default:
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
}

func (c *KVClient) Delete(key string) error {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/delete?key=%s", c.BaseURL, url.QueryEscape(key)), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (c *KVClient) RangeQuery(startKey, endKey string) (map[string]string, error) {
	resp, err := http.Get(fmt.Sprintf("%s/range?startKey=%s&endKey=%s", c.BaseURL, url.QueryEscape(startKey), url.QueryEscape(endKey)))
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var result map[string]string
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return result, nil
}


func (c *KVClient) ScanValueByKey(prefix string, cursor string, limit int) ([]map[string]string, string, error) {
    url := fmt.Sprintf("%s/scanvaluebykey?prefix=%s&cursor=%s&limit=%d", 
        c.BaseURL, url.QueryEscape(prefix), url.QueryEscape(cursor), limit)
    
    resp, err := http.Get(url)
    if err != nil {
        return nil, "", fmt.Errorf("failed to send request: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return nil, "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
    }

    var result struct {
        Results    []map[string]string `json:"results"`
        NextCursor string              `json:"nextCursor"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return nil, "", fmt.Errorf("failed to decode response: %v", err)
    }

    return result.Results, result.NextCursor, nil
}


func (c *KVClient) ScanKey(prefix, cursor string, limit int)(*KVResult, error){
    url := fmt.Sprintf("%s/scankey?prefix=%s&cursor=%s&limit=%d",
    c.BaseURL, url.QueryEscape(prefix),url.QueryEscape(cursor),limit)

    resp, err := http.Get(url)

    if err != nil{
        return nil, fmt.Errorf("failed to send request: %v", err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
    }

    var res KVResult
    if err := json.NewDecoder(resp.Body).Decode(&res); err!=nil{
        return nil, fmt.Errorf("failed to decode response: %v", err)
    }
    return &res, nil
}

func (c *KVClient) ScanOffset(prefix string, offset int)(string, error){
    url := fmt.Sprintf("%s/scanoffset?prefix=%s&offset=%d",
    c.BaseURL, url.QueryEscape(prefix),offset)

    resp, err := http.Get(url)

    if err != nil{
        return "", fmt.Errorf("failed to send request: %v", err)
    }

    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK{
        return "", fmt.Errorf("failed to send ")
    }

    var cursor string
    if err := json.NewDecoder(resp.Body).Decode(&cursor); err != nil {
        return "", fmt.Errorf("failed to decode response: %v", err)
    }

    return cursor, nil
}

func (c *KVClient) TotalKey(prefix string) (int, error){
    url := fmt.Sprintf("%s/totalkey?prefix=%s",
    c.BaseURL, url.QueryEscape(prefix))

    resp, err := http.Get(url)

    if err != nil{
        return 0, fmt.Errorf("failed to send request: %v", err)
    }

    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK{
        return 0, fmt.Errorf("failed to send ")
    }

    var total int
    if err := json.NewDecoder(resp.Body).Decode(&total); err != nil {
        return 0, fmt.Errorf("failed to decode response: %v", err)
    }

    return total, nil
}