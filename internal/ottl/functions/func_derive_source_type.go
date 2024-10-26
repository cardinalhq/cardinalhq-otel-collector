package functions

import (
	"context"
	"fmt"
	"github.com/elastic/go-grok"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
)

type SourceTypeConfig struct {
	GrokExpr        string   `yaml:"grokExpr"`
	Type            string   `yaml:"type"`
	MandatoryFields []string `yaml:"mandatoryFields"`
}

type PatternConfig struct {
	ReceiverType string             `yaml:"receiverType"`
	SourceTypes  []SourceTypeConfig `yaml:"sourceTypes"`
}

type Config struct {
	Patterns []PatternConfig `yaml:"patterns"`
}

func loadConfig(filePath string) (*Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}
	return &config, nil
}

type GrokPattern struct {
	CompiledPattern *grok.Grok
	SourceType      string
	MandatoryFields map[string]string
}

func compilePatterns(config *Config) (map[string][]GrokPattern, error) {
	g, err := grok.NewComplete()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize grok parser: %w", err)
	}

	var compiledPatterns = make(map[string][]GrokPattern)
	for _, patternConfig := range config.Patterns {
		for _, sourceType := range patternConfig.SourceTypes {
			err := g.Compile(sourceType.GrokExpr, true) // assuming named captures only
			if err != nil {
				return nil, fmt.Errorf("failed to compile pattern %q: %w", sourceType.GrokExpr, err)
			}

			mandatoryFields := make(map[string]string, len(sourceType.MandatoryFields))
			for _, field := range sourceType.MandatoryFields {
				mandatoryFields[field] = field
			}

			compiledPatterns[patternConfig.ReceiverType] = append(compiledPatterns[patternConfig.ReceiverType], GrokPattern{
				CompiledPattern: g,
				SourceType:      sourceType.Type,
				MandatoryFields: mandatoryFields,
			})
		}
	}
	return compiledPatterns, nil
}

type DeriveSourceTypeArguments[K any] struct {
	Target       ottl.StringGetter[K]
	ReceiverType ottl.StringGetter[K]
}

func NewDeriveSourceTypeFactory[K any]() ottl.Factory[K] {
	return ottl.NewFactory("DeriveSourceType", &DeriveSourceTypeArguments[K]{}, func(ctx ottl.FunctionContext, args ottl.Arguments) (ottl.ExprFunc[K], error) {
		deriveArgs, ok := args.(*DeriveSourceTypeArguments[K])
		if !ok {
			return nil, fmt.Errorf("DeriveSourceType args must be of type *DeriveSourceTypeArguments[K]")
		}

		// Get the working directory and locate config.yaml in sourcetypes folder
		dir, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("error getting working directory: %v", err)
		}
		configPath := filepath.Join(dir, "functions", "metadata", "source_types.yaml")

		// Load and compile the patterns from config.yaml
		config, err := loadConfig(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load pattern config: %w", err)
		}

		compiledPatterns, err := compilePatterns(config)
		if err != nil {
			return nil, fmt.Errorf("failed to compile patterns: %w", err)
		}

		return deriveSourceType(compiledPatterns, deriveArgs.Target, deriveArgs.ReceiverType), nil
	})
}

func deriveSourceType[K any](patternsByReceiverType map[string][]GrokPattern, target ottl.StringGetter[K], receiverTypeTarget ottl.StringGetter[K]) ottl.ExprFunc[K] {
	return func(ctx context.Context, tCtx K) (any, error) {
		receiverType, err := receiverTypeTarget.Get(ctx, tCtx)
		if err != nil {
			return nil, fmt.Errorf("receiver type is not set or could not be retrieved")
		}

		if _, ok := patternsByReceiverType[receiverType]; !ok {
			return nil, fmt.Errorf("no patterns found for receiver type %q", receiverType)
		}

		patterns := patternsByReceiverType[receiverType]

		val, err := target.Get(ctx, tCtx)
		if err != nil {
			return nil, err
		}

		result := pcommon.NewMap()
		for _, pattern := range patterns {
			matches, err := pattern.CompiledPattern.ParseTypedString(val)
			if err != nil {
				continue
			}

			// Check if all mandatory fields are present in the matches
			missingField := false
			for _, field := range pattern.MandatoryFields {
				if _, ok := matches[field]; !ok {
					missingField = true
					break
				}
			}

			if missingField {
				continue
			}

			for k, v := range matches {
				switch val := v.(type) {
				case bool:
					result.PutBool(k, val)
				case float64:
					result.PutDouble(k, val)
				case int:
					result.PutInt(k, int64(val))
				case string:
					result.PutStr(k, val)
				}
			}
			// Set source type and break. No need to match more sourceTypes.
			result.PutStr("sourceType", pattern.SourceType)
			break
		}
		return result, nil
	}
}
