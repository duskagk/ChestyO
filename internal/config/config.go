package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type Config struct {
	KVServer struct {
		Server struct {
			Host string `yaml:"host"`
			Port int    `yaml:"port"`
		} `yaml:"server"`
	} `yaml:"kvserver"`
}


func LoadConfig(fileName string) (*Config, error) {
    // 실행 파일의 디렉토리 찾기
    exePath, err := os.Executable()
    if err != nil {
        return nil, fmt.Errorf("failed to get executable path: %v", err)
    }
    exeDir := filepath.Dir(exePath)

    // 설정 파일의 전체 경로 생성
    configPath := filepath.Join(exeDir, fileName)

    // 파일 읽기
    file, err := os.Open(configPath)
    if err != nil {
        return nil, fmt.Errorf("failed to open config file %s: %v", configPath, err)
    }
    defer file.Close()

    // YAML 디코딩
    var cfg Config
    decoder := yaml.NewDecoder(file)
    if err := decoder.Decode(&cfg); err != nil {
        return nil, fmt.Errorf("failed to decode config file: %v", err)
    }

    return &cfg, nil
}

